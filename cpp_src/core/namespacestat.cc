
#include "namespacestat.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/jsontools.h"

namespace reindexer {

void NamespaceMemStat::GetJSON(WrSerializer &ser) {
	JsonBuilder builder(ser);

	builder.Put("name", name);
	builder.Put("items_count", itemsCount);

	if (emptyItemsCount) builder.Put("empty_items_count", emptyItemsCount);

	builder.Put("data_size", dataSize);
	builder.Put("storage_ok", storageOK);
	builder.Put("storage_path", storagePath);

	builder.Put("storage_loaded", storageLoaded);

	builder.Object("total").Put("data_size", Total.dataSize).Put("indexes_size", Total.indexesSize).Put("cache_size", Total.cacheSize);

	{
		auto obj = builder.Object("replication");
		replication.GetJSON(obj);
	}

	{
		auto obj = builder.Object("join_cache");
		joinCache.GetJSON(obj);
	}
	{
		auto obj = builder.Object("query_cache");
		queryCache.GetJSON(obj);
	}

	auto arr = builder.Array("indexes");
	for (auto &index : indexes) {
		auto obj = arr.Object();
		index.GetJSON(obj);
	}
}

void LRUCacheMemStat::GetJSON(JsonBuilder &builder) {
	builder.Put("total_size", totalSize);
	builder.Put("items_count", itemsCount);
	builder.Put("empty_count", emptyCount);
	builder.Put("hit_count_limit", hitCountLimit);
}

void IndexMemStat::GetJSON(JsonBuilder &builder) {
	if (uniqKeysCount) builder.Put("uniq_keys_count", uniqKeysCount);
	if (dataSize) builder.Put("data_size", dataSize);
	if (idsetBTreeSize) builder.Put("idset_btree_size", idsetBTreeSize);
	if (idsetPlainSize) builder.Put("idset_plain_size", idsetPlainSize);
	if (sortOrdersSize) builder.Put("sort_orders_size", sortOrdersSize);
	if (fulltextSize) builder.Put("fulltext_size", fulltextSize);
	if (columnSize) builder.Put("column_size", columnSize);

	if (idsetCache.totalSize || idsetCache.itemsCount || idsetCache.emptyCount || idsetCache.hitCountLimit) {
		auto obj = builder.Object("idset_cache");
		idsetCache.GetJSON(obj);
	}

	builder.Put("name", name);
}

void PerfStat::GetJSON(JsonBuilder &builder) {
	builder.Put("total_queries_count", totalHitCount);
	builder.Put("total_avg_latency_us", totalTimeUs);
	builder.Put("total_avg_lock_time_us", totalLockTimeUs);
	builder.Put("last_sec_qps", avgHitCount);
	builder.Put("last_sec_avg_lock_time_us", avgLockTimeUs);
	builder.Put("last_sec_avg_latency_us", avgTimeUs);
	builder.Put("latency_stddev", stddev);
	builder.Put("min_latency_us", minTimeUs);
	builder.Put("max_latency_us", maxTimeUs);
}

void NamespacePerfStat::GetJSON(WrSerializer &ser) {
	JsonBuilder builder(ser);

	builder.Put("name", name);
	{
		auto obj = builder.Object("updates");
		updates.GetJSON(obj);
	}
	{
		auto obj = builder.Object("selects");
		selects.GetJSON(obj);
	}

	auto arr = builder.Array("indexes");

	for (unsigned i = 0; i < indexes.size(); i++) {
		auto obj = arr.Object();
		indexes[i].GetJSON(obj);
	}
}

void IndexPerfStat::GetJSON(JsonBuilder &builder) {
	builder.Put("name", name);
	{
		auto obj = builder.Object("selects");
		selects.GetJSON(obj);
	}
	{
		auto obj = builder.Object("commits");
		commits.GetJSON(obj);
	}
}

void MasterState::GetJSON(JsonBuilder &builder) {
	builder.Put("last_lsn", lastLsn);
	builder.Put("data_hash", dataHash);
	builder.Put("data_count", dataCount);
	builder.Put("updated_unix_nano", int64_t(updatedUnixNano));
}

void MasterState::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		lastLsn = root["last_lsn"].As<int64_t>();
		dataHash = root["data_hash"].As<uint64_t>();
		dataCount = root["data_count"].As<int>();
		updatedUnixNano = root["updated_unix_nano"].As<int64_t>();
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "MasterState: %s", ex.what());
	}
}

static string_view replicationStatusToStr(ReplicationState::Status status) {
	switch (status) {
		case ReplicationState::Status::Idle:
			return "idle"_sv;
		case ReplicationState::Status::Error:
			return "error"_sv;
		case ReplicationState::Status::Fatal:
			return "fatal"_sv;
		case ReplicationState::Status::Syncing:
			return "syncing"_sv;
		case ReplicationState::Status::None:
		default:
			return "none"_sv;
	}
}

static ReplicationState::Status strToReplicationStatus(string_view status) {
	if (status == "idle"_sv) {
		return ReplicationState::Status::Idle;
	} else if (status == "error"_sv) {
		return ReplicationState::Status::Error;
	} else if (status == "fatal"_sv) {
		return ReplicationState::Status::Fatal;
	} else if (status == "syncing"_sv) {
		return ReplicationState::Status::Syncing;
	}
	return ReplicationState::Status::None;
}

void ReplicationState::GetJSON(JsonBuilder &builder) {
	builder.Put("last_lsn", lastLsn);
	builder.Put("slave_mode", slaveMode);
	builder.Put("temporary", temporary);
	builder.Put("incarnation_counter", incarnationCounter);
	builder.Put("data_hash", dataHash);
	builder.Put("data_count", dataCount);
	builder.Put("updated_unix_nano", int64_t(updatedUnixNano));
	builder.Put("status", replicationStatusToStr(status));
	if (slaveMode) {
		builder.Put("error_code", replError.code());
		builder.Put("error_message", replError.what());
		auto masterObj = builder.Object("master_state");
		masterState.GetJSON(masterObj);
	}
}

void ReplicationState::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		lastLsn = root["last_lsn"].As<int64_t>();
		slaveMode = root["slave_mode"].As<bool>();
		temporary = root["temporary"].As<bool>();
		incarnationCounter = root["incarnation_counter"].As<int>();
		dataHash = root["data_hash"].As<uint64_t>();
		dataCount = root["data_count"].As<int>();
		updatedUnixNano = root["updated_unix_nano"].As<int64_t>();
		status = strToReplicationStatus(root["status"].As<string_view>());
		if (slaveMode) {
			int errCode = root["error_code"].As<int>();
			replError = Error(errCode, root["error_message"].As<std::string>());
			try {
				masterState.FromJSON(root["master_state"].As<string_view>());
			} catch (const Error &) {
			} catch (const gason::Exception &) {
			}
		}
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "ReplicationState: %s", ex.what());
	}
}

void ReplicationStat::GetJSON(JsonBuilder &builder) {
	ReplicationState::GetJSON(builder);
	if (!slaveMode) {
		builder.Put("wal_count", walCount);
		builder.Put("wal_size", walSize);
	}
}

}  // namespace reindexer
