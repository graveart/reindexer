#include "nscloner.h"
#include "storage/storagefactory.h"
#include "tools/fsops.h"
#include "tools/logger.h"

namespace reindexer {

#define handleInvalidation(Fn) nsFuncWrapper<decltype(&Fn), &Fn>

void NSCloner::CommitTransaction(Transaction& tx, QueryResults& result, const RdxContext& ctx) {
	auto ns = atomicLoadMainNs();
	if (needNamespaceCopy(ns, tx)) {
		PerfStatCalculatorMT calc(ns->updatePerfCounter_, ns->enablePerfCounters_);
		contexted_unique_lock<Mutex, const RdxContext> lck(clonerMtx_, &ctx);
		ns = ns_;
		if (needNamespaceCopy(ns, tx)) {
			calc.SetCounter(ns->updatePerfCounter_);
			calc.LockHit();
			logPrintf(LogTrace, "NSCloner::CommitTransaction creating copy for (%s)", ns->name_);
			hasCopy_.store(true, std::memory_order_release);
			ns->cancelCommit_ = true;  // -V519
			try {
				contexted_shared_lock<Namespace::Mutex, const RdxContext> lck(ns->mtx_, &ctx);
				std::lock_guard<std::mutex> storageLock(ns->storage_mtx_);
				ns->cancelCommit_ = false;	// -V519
				nsCopy_.reset(new Namespace(*ns));
				calc.SetCounter(nsCopy_->updatePerfCounter_);
				nsCopy_->CommitTransaction(tx, result, NsContext(ctx).NoLock());
				ns->invalidate();
				atomicStoreMainNs(nsCopy_.release());
				hasCopy_.store(false, std::memory_order_release);
			} catch (...) {
				nsCopy_.reset();
				hasCopy_.store(false, std::memory_order_release);
				throw;
			}
			return;
		}
	}
	handleInvalidation(Namespace::CommitTransaction)(tx, result, NsContext(ctx));
}

void NSCloner::doRename(NSCloner::Ptr dst, const std::string& newName, const std::string& storagePath, const RdxContext& ctx) {
	std::string dbpath;
	handleInvalidation(Namespace::flushStorage)(ctx);
	auto lck = nsFuncWrapper<Namespace::WLock (Namespace::*)(const RdxContext&), &Namespace::createWLock>(ctx);
	auto& srcNs = *atomicLoadMainNs();	// -V758
	Namespace::Mutex* dstMtx = nullptr;
	Namespace::Ptr dstNs;
	if (dst) {
		while (true) {
			try {
				dstNs = dst->awaitMainNs(ctx);
				dstMtx = dstNs->createWLock(ctx).release();
				break;
			} catch (const Error& e) {
				if (e.code() != errNamespaceInvalidated) {
					throw;
				} else {
					std::this_thread::yield();
				}
			}
		}
		dbpath = dstNs->dbpath_;
	} else if (newName == srcNs.name_) {
		return;
	}

	if (dbpath.empty()) {
		dbpath = fs::JoinPath(storagePath, newName);
	} else {
		dstNs->deleteStorage();
	}

	bool hadStorage = (srcNs.storage_ != nullptr);
	auto storageType = StorageType::LevelDB;
	if (hadStorage) {
		storageType = srcNs.storage_->Type();
		srcNs.storage_.reset();
		fs::RmDirAll(dbpath);
		int renameRes = fs::Rename(srcNs.dbpath_, dbpath);
		if (renameRes < 0) {
			if (dst) {
				assert(dstMtx);
				dstMtx->unlock();
			}
			throw Error(errParams, "Unable to rename '%s' to '%s'", srcNs.dbpath_, dbpath);
		}
	}
	if (dst) {
		srcNs.name_ = dstNs->name_;
		assert(dstMtx);
		dstMtx->unlock();
	} else {
		srcNs.name_ = newName;
	}

	if (hadStorage) {
		logPrintf(LogTrace, "Storage was moved from %s to %s", srcNs.dbpath_, dbpath);
		srcNs.dbpath_ = std::move(dbpath);
		srcNs.storage_.reset(datastorage::StorageFactory::create(storageType));
		auto status = srcNs.storage_->Open(srcNs.dbpath_, srcNs.storageOpts_);
		if (!status.ok()) {
			throw status;
		}
		if (srcNs.repl_.temporary) {
			srcNs.repl_.temporary = false;
			srcNs.saveReplStateToStorage();
		}
	}
}

}  // namespace reindexer
