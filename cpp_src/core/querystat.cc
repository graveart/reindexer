
#include "querystat.h"

#include "core/cjson/jsonbuilder.h"
#include "query/query.h"

namespace reindexer {

template <void (PerfStatCounterST::*hitFunc)(std::chrono::microseconds)>
void QueriesStatTracer::hit(const Query &q, std::chrono::microseconds time) {
	WrSerializer serNorm, serNonNorm;
	const string_view sqlq{q.GetSQL(serNorm, true).Slice()};
	std::unique_lock<std::mutex> lck(mtx_);
	const auto it = stat_.find(sqlq);
	if (it == stat_.end()) {
		(stat_.emplace(string(sqlq), Stat(q.GetSQL(serNonNorm, false).Slice())).first->second.*hitFunc)(time);
	} else {
		const auto maxTime = it->second.MaxTime();
		(it->second.*hitFunc)(time);
		if (it->second.MaxTime() > maxTime) {
			it->second.longestQuery = string(q.GetSQL(serNonNorm, false).Slice());
		}
	}
};
template void QueriesStatTracer::hit<&PerfStatCounterST::Hit>(const Query &, std::chrono::microseconds);
template void QueriesStatTracer::hit<&PerfStatCounterST::LockHit>(const Query &, std::chrono::microseconds);

const std::vector<QueryPerfStat> QueriesStatTracer::Data() {
	std::unique_lock<std::mutex> lck(mtx_);

	std::vector<QueryPerfStat> ret;
	ret.reserve(stat_.size());
	for (auto &stat : stat_) ret.push_back({stat.first, stat.second.Get<PerfStat>(), stat.second.longestQuery});
	return ret;
}

void QueryPerfStat::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);

	builder.Put("query", query);
	builder.Put("total_queries_count", perf.totalHitCount);
	builder.Put("total_avg_lock_time_us", perf.totalLockTimeUs);
	builder.Put("total_avg_latency_us", perf.totalTimeUs);
	builder.Put("last_sec_qps", perf.avgHitCount);
	builder.Put("last_sec_avg_lock_time_us", perf.avgLockTimeUs);
	builder.Put("last_sec_avg_latency_us", perf.avgTimeUs);
	builder.Put("latency_stddev", perf.stddev);
	builder.Put("min_latency_us", perf.minTimeUs);
	builder.Put("max_latency_us", perf.maxTimeUs);
	builder.Put("longest_query", longestQuery);
}

}  // namespace reindexer
