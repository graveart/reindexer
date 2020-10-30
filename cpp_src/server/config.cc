#include "config.h"

#include "args/args.hpp"
#include "core/storage/storagefactory.h"
#include "tools/fsops.h"
#include "yaml/yaml.h"

namespace reindexer_server {

void ServerConfig::Reset() {
	args_.clear();
	WebRoot.clear();
	StorageEngine = "leveldb";
	HTTPAddr = "0.0.0.0:9088";
	RPCAddr = "0.0.0.0:6534";
	LogLevel = "info";
	ServerLog = "stdout";
	CoreLog = "stdout";
	HttpLog = "stdout";
	RpcLog = "stdout";
#ifndef _WIN32
	StoragePath = "/tmp/reindex";
	UserName.clear();
	DaemonPidFile = "reindexer.pid";
	Daemonize = false;
#else
	StoragePath = "\\reindexer";
	InstallSvc = false;
	RemoveSvc = false;
	SvcMode = false;
#endif
	StartWithErrors = false;
	EnableSecurity = false;
	DebugPprof = false;
	EnablePrometheus = false;
	PrometheusCollectPeriod = std::chrono::milliseconds(1000);
	DebugAllocs = false;
	Autorepair = false;
	EnableConnectionsStats = true;
	TxIdleTimeout = std::chrono::seconds(600);
}

reindexer::Error ServerConfig::ParseYaml(const std::string &yaml) {
	Error err;
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		err = fromYaml(root);
	} catch (const Yaml::Exception &ex) {
		err = Error(errParams, "Error with config string. Reason: '%s'", ex.Message());
	}
	return err;
}

Error ServerConfig::ParseFile(const std::string &filePath) {
	Error err;
	Yaml::Node root;
	try {
		Yaml::Parse(root, filePath.c_str());
		err = fromYaml(root);
	} catch (const Yaml::Exception &ex) {
		err = Error(errParams, "Error with config file '%s'. Reason: %s", filePath, ex.Message());
	}
	return err;
}

Error ServerConfig::ParseCmd(int argc, char *argv[]) {
	using reindexer::fs::GetDirPath;
#ifndef LINK_RESOURCES
	WebRoot = GetDirPath(argv[0]);
#endif
	args_.assign(argv, argv + argc);

	args::ArgumentParser parser("reindexer server");
	args::HelpFlag help(parser, "help", "Show this message", {'h', "help"});
	args::Flag securityF(parser, "", "Enable per-user security", {"security"});
	args::ValueFlag<string> configF(parser, "CONFIG", "Path to reindexer config file", {'c', "config"}, args::Options::Single);
	args::Flag startWithErrorsF(parser, "", "Allow to start reindexer with DB's load erros", {"startwitherrors"});

	args::Group dbGroup(parser, "Database options");
	args::ValueFlag<string> storageF(dbGroup, "PATH", "path to 'reindexer' storage", {'s', "db"}, StoragePath, args::Options::Single);
	auto availableStorageTypes = reindexer::datastorage::StorageFactory::getAvailableTypes();
	std::string availabledStorages;
	for (const auto &type : availableStorageTypes) {
		if (!availabledStorages.empty()) {
			availabledStorages.append(", ");
		}
		availabledStorages.append("'" + reindexer::datastorage::StorageTypeToString(type) + "'");
	}
	args::ValueFlag<string> storageEngineF(dbGroup, "NAME", "'reindexer' storage engine (" + availabledStorages + ")", {'e', "engine"},
										   StorageEngine, args::Options::Single);
	args::Flag autorepairF(dbGroup, "", "Enable autorepair for storages after unexpected shutdowns", {"autorepair"});

	args::Group netGroup(parser, "Network options");
	args::ValueFlag<string> httpAddrF(netGroup, "PORT", "http listen host:port", {'p', "httpaddr"}, HTTPAddr, args::Options::Single);
	args::ValueFlag<string> rpcAddrF(netGroup, "RPORT", "RPC listen host:port", {'r', "rpcaddr"}, RPCAddr, args::Options::Single);
	args::ValueFlag<string> webRootF(netGroup, "PATH", "web root", {'w', "webroot"}, WebRoot, args::Options::Single);
	args::Flag pprofF(netGroup, "", "Enable pprof http handler", {'f', "pprof"});
	args::ValueFlag<int> txIdleTimeoutF(dbGroup, "", "http transactions idle timeout (s)", {"tx-idle-timeout"}, TxIdleTimeout.count(),
										args::Options::Single);

	args::Group metricsGroup(parser, "Metrics options");
	args::Flag prometheusF(metricsGroup, "", "Enable prometheus handler", {"prometheus"});
	args::ValueFlag<int> prometheusPeriodF(metricsGroup, "", "Prometheus stats collect period (ms)", {"prometheus-period"},
										   PrometheusCollectPeriod.count(), args::Options::Single);
	args::Flag clientsConnectionsStatF(metricsGroup, "", "Enable client connection statistic", {"clientsstats"});

	args::Group logGroup(parser, "Logging options");
	args::ValueFlag<string> logLevelF(logGroup, "", "log level (none, warning, error, info, trace)", {'l', "loglevel"}, LogLevel,
									  args::Options::Single);
	args::ValueFlag<string> serverLogF(logGroup, "", "Server log file", {"serverlog"}, ServerLog, args::Options::Single);
	args::ValueFlag<string> coreLogF(logGroup, "", "Core log file", {"corelog"}, CoreLog, args::Options::Single);
	args::ValueFlag<string> httpLogF(logGroup, "", "Http log file", {"httplog"}, HttpLog, args::Options::Single);
	args::ValueFlag<string> rpcLogF(logGroup, "", "Rpc log file", {"rpclog"}, RpcLog, args::Options::Single);
	args::Flag logAllocsF(netGroup, "", "Log operations allocs statistics", {'a', "allocs"});

#ifndef _WIN32
	args::Group unixDaemonGroup(parser, "Unix daemon options");
	args::ValueFlag<string> userF(unixDaemonGroup, "USER", "System user name", {'u', "user"}, UserName, args::Options::Single);
	args::Flag daemonizeF(unixDaemonGroup, "", "Run in daemon mode", {'d', "daemonize"});
	args::ValueFlag<string> daemonPidFileF(unixDaemonGroup, "", "Custom daemon pid file", {"pidfile"}, DaemonPidFile,
										   args::Options::Single);
#else
	args::Group winSvcGroup(parser, "Windows service options");
	args::Flag installF(winSvcGroup, "", "Install reindexer windows service", {"install"});
	args::Flag removeF(winSvcGroup, "", "Remove reindexer windows service", {"remove"});
	args::Flag serviceF(winSvcGroup, "", "Run in service mode", {"service"});
#endif

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help &) {
		return Error(errLogic, parser.Help());
	} catch (const args::Error &e) {
		return Error(errParams, "%s\n%s", e.what(), parser.Help());
	}

	if (configF) {
		auto err = ParseFile(args::get(configF));
		if (!err.ok()) return err;
	}

	if (storageF) StoragePath = args::get(storageF);
	if (storageEngineF) StorageEngine = args::get(storageEngineF);
	if (startWithErrorsF) StartWithErrors = args::get(startWithErrorsF);
	if (autorepairF) Autorepair = args::get(autorepairF);
	if (logLevelF) LogLevel = args::get(logLevelF);
	if (httpAddrF) HTTPAddr = args::get(httpAddrF);
	if (rpcAddrF) RPCAddr = args::get(rpcAddrF);
	if (webRootF) WebRoot = args::get(webRootF);
#ifndef _WIN32
	if (userF) UserName = args::get(userF);
	if (daemonizeF) Daemonize = args::get(daemonizeF);
	if (daemonPidFileF) DaemonPidFile = args::get(daemonPidFileF);
#else
	if (installF) InstallSvc = args::get(installF);
	if (removeF) RemoveSvc = args::get(removeF);
	if (serviceF) SvcMode = args::get(serviceF);

#endif
	if (securityF) EnableSecurity = args::get(securityF);
	if (serverLogF) ServerLog = args::get(serverLogF);
	if (coreLogF) CoreLog = args::get(coreLogF);
	if (httpLogF) HttpLog = args::get(httpLogF);
	if (rpcLogF) RpcLog = args::get(rpcLogF);
	if (pprofF) DebugPprof = args::get(pprofF);
	if (prometheusF) EnablePrometheus = args::get(prometheusF);
	if (prometheusPeriodF) PrometheusCollectPeriod = std::chrono::milliseconds(args::get(prometheusPeriodF));
	if (clientsConnectionsStatF) EnableConnectionsStats = args::get(clientsConnectionsStatF);
	if (logAllocsF) DebugAllocs = args::get(logAllocsF);
	if (txIdleTimeoutF) TxIdleTimeout = std::chrono::seconds(args::get(txIdleTimeoutF));

	return 0;
}

reindexer::Error ServerConfig::fromYaml(Yaml::Node &root) {
	try {
		StoragePath = root["storage"]["path"].As<std::string>(StoragePath);
		StorageEngine = root["storage"]["engine"].As<std::string>(StorageEngine);
		StartWithErrors = root["storage"]["startwitherrors"].As<bool>(StartWithErrors);
		Autorepair = root["storage"]["autorepair"].As<bool>(Autorepair);
		LogLevel = root["logger"]["loglevel"].As<std::string>(LogLevel);
		ServerLog = root["logger"]["serverlog"].As<std::string>(ServerLog);
		CoreLog = root["logger"]["corelog"].As<std::string>(CoreLog);
		HttpLog = root["logger"]["httplog"].As<std::string>(HttpLog);
		RpcLog = root["logger"]["rpclog"].As<std::string>(RpcLog);
		HTTPAddr = root["net"]["httpaddr"].As<std::string>(HTTPAddr);
		RPCAddr = root["net"]["rpcaddr"].As<std::string>(RPCAddr);
		WebRoot = root["net"]["webroot"].As<std::string>(WebRoot);
		EnableSecurity = root["net"]["security"].As<bool>(EnableSecurity);
		TxIdleTimeout = std::chrono::seconds(root["net"]["tx_idle_timeout"].As<int>(TxIdleTimeout.count()));
		EnablePrometheus = root["metrics"]["prometheus"].As<bool>(EnablePrometheus);
		PrometheusCollectPeriod = std::chrono::milliseconds(root["metrics"]["collect_period"].As<int>(PrometheusCollectPeriod.count()));
		EnableConnectionsStats = root["metrics"]["clientsstats"].As<bool>(EnableConnectionsStats);
#ifndef _WIN32
		UserName = root["system"]["user"].As<std::string>(UserName);
		Daemonize = root["system"]["daemonize"].As<bool>(Daemonize);
		DaemonPidFile = root["system"]["pidfile"].As<std::string>(DaemonPidFile);
#endif
		DebugAllocs = root["debug"]["allocs"].As<bool>(DebugAllocs);
		DebugPprof = root["debug"]["pprof"].As<bool>(DebugPprof);
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "%s", ex.Message());
	}
	return 0;
}

}  // namespace reindexer_server
