#pragma once

#include <string>
#include <thread>
#include "core/dbconfig.h"
#include "core/namespace/namespace.h"
#include "core/namespace/namespacestat.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/fast_hash_map.h"
#include "net/ev/ev.h"
#include "tools/errors.h"
#include "updatesobserver.h"

namespace reindexer {
using std::string;
namespace client {
class Reindexer;
class QueryResults;
}  // namespace client

class ReindexerImpl;

class Replicator : public IUpdatesObserver {
public:
	Replicator(ReindexerImpl *slave);
	~Replicator();
	bool Configure(const ReplicationConfigData &config);
	Error Start();
	void Stop();
	void Enable() { enabled_.store(true, std::memory_order_release); }

protected:
	struct SyncStat {
		ReplicationState masterState;
		Error lastError;
		int updated = 0, deleted = 0, errors = 0, updatedIndexes = 0, deletedIndexes = 0, updatedMeta = 0, processed = 0, schemasSet = 0;
		WrSerializer &Dump(WrSerializer &ser);
	};
	struct NsErrorMsg {
		Error err;
		uint64_t count = 0;
	};

	void run();
	void stop();
	// Sync single namespace
	Error syncNamespace(const NamespaceDef &ns, string_view forceSyncReason);
	// Sync database
	Error syncDatabase();
	// Read and apply WAL from master
	Error syncNamespaceByWAL(const NamespaceDef &ns);
	// Apply WAL from master to namespace
	Error applyWAL(Namespace::Ptr slaveNs, client::QueryResults &qr);
	// Sync indexes of namespace
	Error syncIndexesForced(Namespace::Ptr slaveNs, const NamespaceDef &masterNsDef);
	// Sync namespace schema
	Error syncSchemaForced(Namespace::Ptr slaveNs, const NamespaceDef &masterNsDef);
	// Forced sync of namespace
	Error syncNamespaceForced(const NamespaceDef &ns, string_view reason);
	// Sync meta data
	Error syncMetaForced(reindexer::Namespace::Ptr slaveNs, string_view nsName);
	// Apply single WAL record
	Error applyWALRecord(LSNPair LSNs, string_view nsName, Namespace::Ptr ns, const WALRecord &wrec, SyncStat &stat);
	// Apply single transaction WAL record
	Error applyTxWALRecord(LSNPair LSNs, string_view nsName, Namespace::Ptr ns, const WALRecord &wrec);
	void checkNoOpenedTransaction(string_view nsName, Namespace::Ptr slaveNs);
	// Apply single cjson item
	Error modifyItem(LSNPair LSNs, Namespace::Ptr ns, string_view cjson, int modifyMode, const TagsMatcher &tm, SyncStat &stat);
	static Error unpackItem(Item &, lsn_t, string_view cjson, const TagsMatcher &tm);

	void OnWALUpdate(LSNPair LSNs, string_view nsName, const WALRecord &walRec) override final;
	void OnConnectionState(const Error &err) override final;

	bool canApplyUpdate(LSNPair LSNs, string_view nsName, const WALRecord &wrec);
	bool isSyncEnabled(string_view nsName);
	bool retryIfNetworkError(const Error &err);

	std::unique_ptr<client::Reindexer> master_;
	ReindexerImpl *slave_;

	net::ev::dynamic_loop loop_;
	std::thread thread_;
	net::ev::async stop_;
	net::ev::async resync_;
	net::ev::timer resyncTimer_;
	net::ev::async walSyncAsync_;

	ReplicationConfigData config_;

	std::atomic<bool> terminate_;
	enum State { StateInit, StateSyncing, StateIdle };
	std::atomic<State> state_;

	using UpdatesContainer = std::vector<std::pair<LSNPair, PackedWALRecord>>;
	fast_hash_map<string, UpdatesContainer, nocase_hash_str, nocase_equal_str> pendedUpdates_;
	tsl::hopscotch_set<string, nocase_hash_str, nocase_equal_str> syncedNamespaces_;
	std::string currentSyncNs_;

	std::mutex syncMtx_;
	std::mutex masterMtx_;
	std::atomic<bool> enabled_;

	const RdxContext dummyCtx_;
	std::unordered_map<const Namespace *, Transaction> transactions_;
	fast_hash_map<string, NsErrorMsg, nocase_hash_str, nocase_equal_str> lastNsErrMsg_;

	class SyncQuery {
	public:
		SyncQuery() {}
		void Push(std::string &nsName, NamespaceDef &nsDef, bool force);
		bool Pop(NamespaceDef &def, bool &force);

	private:
		struct recordData {
			NamespaceDef def;
			bool forced;
		};
		std::unordered_map<std::string, recordData> query_;
		std::mutex mtx_;
	};
	SyncQuery syncQuery_;
};

}  // namespace reindexer
