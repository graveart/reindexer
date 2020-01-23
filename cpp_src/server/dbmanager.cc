#include <fstream>
#include <mutex>

#include <thread>
#include "dbmanager.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/md5crypt.h"
#include "tools/stringstools.h"
#include "vendor/hash/md5.h"
#include "vendor/yaml/yaml.h"

namespace reindexer_server {

const std::string kUsersYAMLFilename = "users.yml";
const std::string kUsersJSONFilename = "users.json";

DBManager::DBManager(const string &dbpath, bool noSecurity)
	: dbpath_(dbpath), noSecurity_(noSecurity), storageType_(datastorage::StorageType::LevelDB) {}

Error DBManager::Init(const std::string &storageEngine, bool allowDBErrors, bool withAutorepair) {
	auto status = readUsers();
	if (!status.ok() && !noSecurity_) {
		return status;
	}

	vector<fs::DirEntry> foundDb;
	if (fs::ReadDir(dbpath_, foundDb) < 0) {
		return Error(errParams, "Can't read reindexer dir %s", dbpath_);
	}

	try {
		storageType_ = datastorage::StorageTypeFromString(storageEngine);
	} catch (const Error &err) {
		return err;
	}

	for (auto &de : foundDb) {
		if (de.isDir && validateObjectName(de.name)) {
			auto status = loadOrCreateDatabase(de.name, allowDBErrors, withAutorepair);
			if (!status.ok()) {
				logPrintf(LogError, "Failed to open database '%s' - %s", de.name, status.what());
				if (status.code() == errNotValid) {
					logPrintf(LogError, "Try to run:\t`reindexer_tool --dsn \"builtin://%s\" --repair`  to restore data", dbpath_);
					return status;
				}
			}
		}
	}

	return 0;
}

Error DBManager::OpenDatabase(const string &dbName, AuthContext &auth, bool canCreate) {
	RdxContext dummyCtx;
	auto status = Login(dbName, auth);
	if (!status.ok()) {
		return status;
	}
	auto dbConnect = [&auth](Reindexer *db) {
		if (auth.checkClusterID_) {
			return db->Connect(std::string(), ConnectOpts().WithExpectedClusterID(auth.expectedClusterID_));
		}
		return Error();
	};

	smart_lock<Mutex> lck(mtx_, dummyCtx);
	auto it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		status = dbConnect(it->second.get());
		if (!status.ok()) return status;
		auth.db_ = it->second.get();
		return errOK;
	}
	lck.unlock();

	if (!canCreate) {
		return Error(errNotFound, "Database '%s' not found", dbName);
	}
	if (auth.role_ < kRoleOwner) {
		return Error(errForbidden, "Forbidden to create database %s", dbName);
	}
	if (!validateObjectName(dbName)) {
		return Error(errParams, "Database name contains invalid character. Only alphas, digits,'_','-, are allowed");
	}

	lck = smart_lock<Mutex>(mtx_, dummyCtx, true);
	it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		status = dbConnect(it->second.get());
		if (!status.ok()) return status;
		auth.db_ = it->second.get();
		return errOK;
	}

	status = loadOrCreateDatabase(dbName, true, true, auth);
	if (!status.ok()) {
		return status;
	}

	it = dbs_.find(dbName);
	assert(it != dbs_.end());
	auth.db_ = it->second.get();
	return errOK;
}

Error DBManager::loadOrCreateDatabase(const string &dbName, bool allowDBErrors, bool withAutorepair, const AuthContext &auth) {
	string storagePath = fs::JoinPath(dbpath_, dbName);

	logPrintf(LogInfo, "Loading database %s", dbName);
	auto db = unique_ptr<reindexer::Reindexer>(new reindexer::Reindexer);
	StorageTypeOpt storageType = kStorageTypeOptLevelDB;
	switch (storageType_) {
		case datastorage::StorageType::LevelDB:
			storageType = kStorageTypeOptLevelDB;
			break;
		case datastorage::StorageType::RocksDB:
			storageType = kStorageTypeOptRocksDB;
			break;
	}
	auto opts = ConnectOpts().AllowNamespaceErrors(allowDBErrors).WithStorageType(storageType).Autorepair(withAutorepair);
	if (auth.checkClusterID_) {
		opts = opts.WithExpectedClusterID(auth.expectedClusterID_);
	}
	auto status = db->Connect(storagePath, opts);
	if (status.ok()) {
		dbs_[dbName] = std::move(db);
	}

	return status;
}

Error DBManager::DropDatabase(AuthContext &auth) {
	{
		Reindexer *db = nullptr;
		auto status = auth.GetDB(kRoleOwner, &db);
		if (!status.ok()) {
			return status;
		}
	}
	string dbName = auth.dbName_;

	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = dbs_.find(auth.dbName_);
	if (it == dbs_.end()) {
		return Error(errParams, "Database %s not found", dbName);
	}
	dbs_.erase(it);
	auth.ResetDB();

	fs::RmDirAll(fs::JoinPath(dbpath_, dbName));
	return 0;
}

vector<string> DBManager::EnumDatabases() {
	shared_lock<shared_timed_mutex> lck(mtx_);
	vector<string> dbs;
	for (auto &it : dbs_) dbs.push_back(it.first);
	return dbs;
}

Error DBManager::Login(const string &dbName, AuthContext &auth) {
	if (kRoleSystem == auth.role_) {
		auth.dbName_ = dbName;
		return 0;
	}

	if (IsNoSecurity()) {
		auth.role_ = kRoleOwner;
		auth.dbName_ = dbName;
		return 0;
	}

	if (auth.role_ != kUnauthorized && dbName == auth.dbName_) {
		return 0;
	}

	auto it = users_.find(auth.login_);
	if (it == users_.end()) {
		return Error(errForbidden, "Unauthorized");
	}
	// TODO change to SCRAM-RSA
	if (!it->second.salt.empty()) {
		if (it->second.hash != reindexer::MD5crypt(auth.password_, it->second.salt)) {
			return Error(errForbidden, "Unauthorized");
		}
	} else if (it->second.hash != auth.password_) {
		return Error(errForbidden, "Unauthorized");
	}

	auth.role_ = kRoleNone;

	if (!dbName.empty()) {
		const UserRecord &urec = it->second;

		auto dbIt = urec.roles.find("*");
		if (dbIt != urec.roles.end()) {
			auth.role_ = dbIt->second;
		}

		dbIt = urec.roles.find(dbName);
		if (dbIt != urec.roles.end() && dbIt->second > auth.role_) {
			auth.role_ = dbIt->second;
		}
	}
	auth.dbName_ = dbName;
	// logPrintf(LogInfo, "Authorized user '%s', to db '%s', role=%s", auth.login_, dbName, UserRoleName(auth.role_));

	return 0;
}

Error DBManager::readUsers() noexcept {
	users_.clear();
	auto result = readUsersYAML();
	if (!result.ok()) {
		result = readUsersJSON();
		if (result.code() == errNotFound) {
			return createDefaultUsersYAML();
		}
	}
	return result;
}

Error DBManager::readUsersYAML() noexcept {
	string content;
	int res = fs::ReadFile(fs::JoinPath(dbpath_, kUsersYAMLFilename), content);
	if (res < 0) return Error(errNotFound, "Can't read '%s' file", kUsersYAMLFilename);
	Yaml::Node root;
	try {
		Yaml::Parse(root, content);
		for (auto userIt = root.Begin(); userIt != root.End(); userIt++) {
			UserRecord urec;
			urec.login = (*userIt).first;
			auto &userNode = (*userIt).second;
			auto err = ParseMd5CryptString(userNode["hash"].As<string>(), urec.hash, urec.salt);
			if (!err.ok()) {
				logPrintf(LogWarning, "Hash parsing error for user '%s': %s", urec.login, err.what());
				continue;
			}
			auto userRoles = userNode["roles"];
			if (userRoles.Type() == Yaml::Node::MapType) {
				for (auto roleIt = userRoles.Begin(); roleIt != userRoles.End(); roleIt++) {
					string db((*roleIt).first);
					try {
						UserRole role = userRoleFromString((*roleIt).second.As<string>());
						urec.roles.emplace(db, role);
					} catch (const Error &err) {
						logPrintf(LogWarning, "Skipping user '%s' for db '%s': ", urec.login, db, err.what());
					}
				}
				if (urec.roles.empty()) {
					logPrintf(LogWarning, "User '%s' doesn't have valid roles", urec.login);
				} else {
					users_.emplace(urec.login, urec);
				}
			} else {
				logPrintf(LogWarning, "Skipping user '%s': no 'roles' node found", urec.login);
			}
		}
	} catch (const Yaml::Exception &ex) {
		return Error(errParseJson, "Users: %s", ex.what());
	}
	return errOK;
}

Error DBManager::readUsersJSON() noexcept {
	string content;
	int res = fs::ReadFile(fs::JoinPath(dbpath_, kUsersJSONFilename), content);
	if (res < 0) return Error(errNotFound, "Can't read '%s' file", kUsersJSONFilename);

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(giftStr(content));
		for (auto &userNode : root) {
			UserRecord urec;
			urec.login = string(userNode.key);
			auto err = ParseMd5CryptString(userNode["hash"].As<string>(), urec.hash, urec.salt);
			if (!err.ok()) {
				logPrintf(LogWarning, "Hash parsing error for user '%s': %s", urec.login, err.what());
				continue;
			}
			for (auto &roleNode : userNode["roles"]) {
				string db(roleNode.key);
				try {
					UserRole role = userRoleFromString(roleNode.As<string_view>());
					urec.roles.emplace(db, role);
				} catch (const Error &err) {
					logPrintf(LogWarning, "Skipping user '%s' for db '%s': ", urec.login, db, err.what());
				}
			}
			if (urec.roles.empty()) {
				logPrintf(LogWarning, "User '%s' doesn't have valid roles", urec.login);
			} else {
				users_.emplace(urec.login, urec);
			}
		}
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "Users: %s", ex.what());
	}
	return errOK;
}

Error DBManager::createDefaultUsersYAML() noexcept {
	logPrintf(LogInfo, "Creating default %s file", kUsersYAMLFilename);
	int res = fs::WriteFile(fs::JoinPath(dbpath_, kUsersYAMLFilename),
							"# List of db's users, their's roles and privileges\n\n"
							"# Username\n"
							"reindexer:\n"
							"  # Hash type(right now '$1' is the only value), salt and hash in BSD MD5 Crypt format\n"
							"  # Hash may be generated via openssl tool - `openssl passwd -1 -salt MySalt MyPassword`\n"
							"  # If hash doesn't start with '$' sign it will be used as raw password itself\n"
							"  hash: $1$rdxsalt$VIR.dzIB8pasIdmyVGV0E/\n"
							"  # User's roles for specific databases, * in place of db name means any database\n"
							"  # Allowed roles:\n"
							"  # 1) data_read - user can read data from database\n"
							"  # 2) data_write - user can write data to database\n"
							"  # 3) db_admin - user can manage database: kRoleDataWrite + create & delete namespaces, modify indexes\n"
							"  # 4) owner - user has all privilegies on database: kRoleDBAdmin + create & drop database\n"
							"  roles:\n"
							"    *: owner\n");
	if (res < 0) {
		return Error(errParams, "Unable to write default config file: %s", strerror(errno));
	}
	users_.emplace("reindexer", UserRecord{"reindexer", "VIR.dzIB8pasIdmyVGV0E/", "rdxsalt", {{"*", kRoleOwner}}});
	return errOK;
}

UserRole DBManager::userRoleFromString(string_view strRole) {
	if (strRole == "data_read"_sv) {
		return kRoleDataRead;
	} else if (strRole == "data_write"_sv) {
		return kRoleDataWrite;
	} else if (strRole == "db_admin"_sv) {
		return kRoleDBAdmin;
	} else if (strRole == "owner"_sv) {
		return kRoleOwner;
	}
	throw Error(errParams, "Role \'%s\' is invalid", strRole);
}

const char *UserRoleName(UserRole role) noexcept {
	switch (role) {
		case kUnauthorized:
			return "unauthoried";
		case kRoleNone:
			return "none";
		case kRoleDataRead:
			return "data_read";
		case kRoleDataWrite:
			return "data_write";
		case kRoleDBAdmin:
			return "db_admin";
		case kRoleOwner:
			return "owner";
		case kRoleSystem:
			return "system";
	}
	return "";
}

}  // namespace reindexer_server
