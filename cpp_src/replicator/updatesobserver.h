#pragma once

#include <mutex>
#include <vector>
#include "core/lsn.h"
#include "estl/h_vector.h"
#include "estl/shared_mutex.h"
#include "estl/string_view.h"
#include "replicator/walrecord.h"
#include "tools/errors.h"
#include "tools/stringstools.h"
#include "vendor/hopscotch/hopscotch_map.h"

namespace reindexer {

class ItemImpl;
struct IndexDef;

class UpdatesFilters {
public:
	class Filter {
	public:
		bool Check() const { return true; }
		void FromJSON(const gason::JsonNode &) {}
		void GetJSON(JsonBuilder &) const {}

		bool operator==(const Filter &) const { return true; }
	};

	void Merge(const UpdatesFilters &rhs);
	void AddFilter(string_view ns, Filter filter);
	bool Check(string_view ns) const;

	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &root);
	void GetJSON(WrSerializer &ser) const;

	bool operator==(const UpdatesFilters &rhs) const;

private:
	using FiltersList = h_vector<Filter, 4>;

	tsl::hopscotch_map<std::string, FiltersList, nocase_hash_str, nocase_equal_str> filters_;
};

class IUpdatesObserver {
public:
	virtual ~IUpdatesObserver() = default;
	virtual void OnWALUpdate(LSNPair LSNs, string_view nsName, const WALRecord &rec) = 0;
	virtual void OnConnectionState(const Error &err) = 0;
};

class UpdatesObservers {
public:
	Error Add(IUpdatesObserver *observer, const UpdatesFilters &filter, SubscriptionOpts opts);
	Error Delete(IUpdatesObserver *observer);

	void OnModifyItem(LSNPair LSNs, string_view nsName, ItemImpl *item, int modifyMode, bool inTransaction);

	void OnWALUpdate(LSNPair LSNs, string_view nsName, const WALRecord &rec);

	void OnConnectionState(const Error &err);
	bool empty() {
		shared_lock<shared_timed_mutex> lck(mtx_);
		return observers_.empty();
	}
	UpdatesFilters GetMergedFilter() const;

protected:
	struct ObserverInfo {
		IUpdatesObserver *ptr;
		UpdatesFilters filters;
	};

	std::vector<ObserverInfo> observers_;
	mutable shared_timed_mutex mtx_;
};

}  // namespace reindexer
