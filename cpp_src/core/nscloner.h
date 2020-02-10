#pragma once

#include <thread>
#include <type_traits>
#include "estl/shared_mutex.h"
#include "namespace.h"

namespace reindexer {

const unsigned int kMinTxStepsToCopy = 10000;
const unsigned int kTxStepsToAlwaysCopy = 150000;

#define handleInvalidation(Fn) nsFuncWrapper<decltype(&Fn), &Fn>

class NSCloner {
public:
	NSCloner(const string &name, UpdatesObservers &observers) : ns_(std::make_shared<Namespace>(name, observers)) {}
	NSCloner(Namespace::Ptr ns) : ns_(std::move(ns)) {}
	typedef shared_ptr<NSCloner> Ptr;

	void CommitTransaction(Transaction &tx, QueryResults &result, const RdxContext &ctx);
	const string &GetName() const { return handleInvalidation(Namespace::GetName)(); }
	bool IsSystem(const RdxContext &ctx) const { return handleInvalidation(Namespace::IsSystem)(ctx); }
	bool IsTemporary(const RdxContext &ctx) const { return handleInvalidation(Namespace::IsTemporary)(ctx); }
	void EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
		handleInvalidation(Namespace::EnableStorage)(path, opts, storageType, ctx);
	}
	void LoadFromStorage(const RdxContext &ctx) { handleInvalidation(Namespace::LoadFromStorage)(ctx); }
	void DeleteStorage(const RdxContext &ctx) { handleInvalidation(Namespace::DeleteStorage)(ctx); }
	uint32_t GetItemsCount() { return handleInvalidation(Namespace::GetItemsCount)(); }
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(Namespace::AddIndex)(indexDef, ctx); }
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(Namespace::UpdateIndex)(indexDef, ctx); }
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(Namespace::DropIndex)(indexDef, ctx); }
	void Insert(Item &item, const RdxContext &ctx) { handleInvalidation(Namespace::Insert)(item, ctx); }
	void Update(Item &item, const RdxContext &ctx) {
		nsFuncWrapper<void (Namespace::*)(Item &, const RdxContext &), &Namespace::Update>(item, ctx);
	}
	void Update(const Query &query, QueryResults &result, const NsContext &ctx) {
		nsFuncWrapper<void (Namespace::*)(const Query &, QueryResults &, const NsContext &ctx), &Namespace::Update>(query, result, ctx);
	}
	void Upsert(Item &item, const NsContext &ctx) { handleInvalidation(Namespace::Upsert)(item, ctx); }
	void Delete(Item &item, const NsContext &ctx) {
		nsFuncWrapper<void (Namespace::*)(Item &, const NsContext &), &Namespace::Delete>(item, ctx);
	}
	void Delete(const Query &query, QueryResults &result, const NsContext &ctx) {
		nsFuncWrapper<void (Namespace::*)(const Query &, QueryResults &, const NsContext &), &Namespace::Delete>(query, result, ctx);
	}
	void Truncate(const NsContext &ctx) { handleInvalidation(Namespace::Truncate)(ctx); }
	void Select(QueryResults &result, SelectCtx &params, const RdxContext &ctx) {
		handleInvalidation(Namespace::Select)(result, params, ctx);
	}
	NamespaceDef GetDefinition(const RdxContext &ctx) { return handleInvalidation(Namespace::GetDefinition)(ctx); }
	NamespaceMemStat GetMemStat(const RdxContext &ctx) { return handleInvalidation(Namespace::GetMemStat)(ctx); }
	NamespacePerfStat GetPerfStat(const RdxContext &ctx) { return handleInvalidation(Namespace::GetPerfStat)(ctx); }
	void ResetPerfStat(const RdxContext &ctx) { handleInvalidation(Namespace::ResetPerfStat)(ctx); }
	vector<string> EnumMeta(const RdxContext &ctx) { return handleInvalidation(Namespace::EnumMeta)(ctx); }
	void BackgroundRoutine(RdxActivityContext *ctx) {
		if (hasCopy_.load(std::memory_order_acquire)) {
			return;
		}
		handleInvalidation(Namespace::BackgroundRoutine)(ctx);
	}
	void CloseStorage(const RdxContext &ctx) { handleInvalidation(Namespace::CloseStorage)(ctx); }
	Transaction NewTransaction(const RdxContext &ctx) { return handleInvalidation(Namespace::NewTransaction)(ctx); }

	Item NewItem(const RdxContext &ctx) { return handleInvalidation(Namespace::NewItem)(ctx); }
	void ToPool(ItemImpl *item) { handleInvalidation(Namespace::ToPool)(item); }
	string GetMeta(const string &key, const RdxContext &ctx) { return handleInvalidation(Namespace::GetMeta)(key, ctx); }
	void PutMeta(const string &key, const string_view &data, const NsContext &ctx) {
		handleInvalidation(Namespace::PutMeta)(key, data, ctx);
	}
	int64_t GetSerial(const string &field) { return handleInvalidation(Namespace::GetSerial)(field); }
	int getIndexByName(const string &index) const {
		return nsFuncWrapper<int (Namespace::*)(const string &) const, &Namespace::getIndexByName>(index);
	}
	bool getIndexByName(const string &name, int &index) const {
		return nsFuncWrapper<bool (Namespace::*)(const string &, int &) const, &Namespace::getIndexByName>(name, index);
	}
	void FillResult(QueryResults &result, IdSet::Ptr ids) const { handleInvalidation(Namespace::FillResult)(result, ids); }
	void EnablePerfCounters(bool enable = true) { handleInvalidation(Namespace::EnablePerfCounters)(enable); }
	ReplicationState GetReplState(const RdxContext &ctx) const { return handleInvalidation(Namespace::GetReplState)(ctx); }
	void SetSlaveLSN(int64_t slaveLSN, const RdxContext &ctx) { handleInvalidation(Namespace::SetSlaveLSN)(slaveLSN, ctx); }
	void SetSlaveReplStatus(ReplicationState::Status status, const Error &error, const RdxContext &ctx) {
		handleInvalidation(Namespace::SetSlaveReplStatus)(status, error, ctx);
	}
	void SetSlaveReplMasterState(MasterState state, const RdxContext &ctx) {
		handleInvalidation(Namespace::SetSlaveReplMasterState)(state, ctx);
	}
	void ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &ctx) { handleInvalidation(Namespace::ReplaceTagsMatcher)(tm, ctx); }
	void Rename(NSCloner::Ptr dst, const std::string &storagePath, const RdxContext &ctx) {
		if (this == dst.get() || dst == nullptr) {
			return;
		}
		doRename(std::move(dst), std::string(), storagePath, ctx);
	}

	void Rename(const std::string &newName, const std::string &storagePath, const RdxContext &ctx) {
		if (newName.empty()) {
			return;
		}
		doRename(nullptr, newName, storagePath, ctx);
	}
	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
		handleInvalidation(Namespace::OnConfigUpdated)(configProvider, ctx);
	}
	void SetStorageOpts(StorageOpts opts, const RdxContext &ctx) { handleInvalidation(Namespace::SetStorageOpts)(opts, ctx); }
	StorageOpts GetStorageOpts(const RdxContext &ctx) { return handleInvalidation(Namespace::GetStorageOpts)(ctx); }
	void Refill(vector<Item> &items, const NsContext &ctx) { handleInvalidation(Namespace::Refill)(items, ctx); }

protected:
	friend class ReindexerImpl;
	bool tryToReload(const RdxContext &ctx) const { return handleInvalidation(Namespace::tryToReload)(ctx); }
	bool needToLoadData(const RdxContext &ctx) const { return handleInvalidation(Namespace::needToLoadData)(ctx); }
	void updateSelectTime() const { handleInvalidation(Namespace::updateSelectTime)(); }

	Namespace::Ptr getMainNs() const { return atomicLoadMainNs(); }
	Namespace::Ptr awaitMainNs(const RdxContext &ctx) const {
		if (hasCopy_.load(std::memory_order_acquire)) {
			contexted_unique_lock<Mutex, const RdxContext> lck(clonerMtx_, &ctx);
			assert(!hasCopy_.load(std::memory_order_acquire));
			return ns_;
		}
		return atomicLoadMainNs();
	}

private:
	template <typename Fn, Fn fn, typename... Args>
	typename std::result_of<Fn(Namespace, Args...)>::type nsFuncWrapper(Args &&... args) const {
		while (true) {
			try {
				auto ns = atomicLoadMainNs();
				return (*ns.*fn)(std::forward<Args>(args)...);
			} catch (const Error &e) {
				if (e.code() != errNamespaceInvalidated) {
					throw;
				} else {
					std::this_thread::yield();
				}
			}
		}
	}

	bool needNamespaceCopy(const Namespace::Ptr &ns, const Transaction &tx) const {
		auto stepsCount = tx.GetSteps().size();
		return ((stepsCount >= kMinTxStepsToCopy) && (ns->GetItemsCapacity() <= 5 * stepsCount)) || (stepsCount >= kTxStepsToAlwaysCopy);
	}
	void doRename(NSCloner::Ptr dst, const std::string &newName, const std::string &storagePath, const RdxContext &ctx);

	Namespace::Ptr atomicLoadMainNs() const {
		std::lock_guard<spinlock> lck(nsPtrSpinlock_);
		return ns_;
	}
	void atomicStoreMainNs(Namespace *ns) {
		std::lock_guard<spinlock> lck(nsPtrSpinlock_);
		ns_.reset(ns);
	}

	std::shared_ptr<Namespace> ns_;
	std::unique_ptr<Namespace> nsCopy_;
	std::atomic<bool> hasCopy_ = {false};
	using Mutex = MarkedMutex<std::timed_mutex, MutexMark::Namespace>;
	mutable Mutex clonerMtx_;
	mutable spinlock nsPtrSpinlock_;
};

#undef handleInvalidation

}  // namespace reindexer
