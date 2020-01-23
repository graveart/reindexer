#include "client/reindexer.h"
#include "client/rpcclient.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

Reindexer::Reindexer(const ReindexerConfig& config) : impl_(new RPCClient(config)), owner_(true), ctx_() {}
Reindexer::~Reindexer() {
	if (owner_) {
		delete impl_;
	}
}
Reindexer::Reindexer(Reindexer&& rdx) noexcept : impl_(rdx.impl_), owner_(rdx.owner_), ctx_(rdx.ctx_) { rdx.owner_ = false; }

Error Reindexer::Connect(const string& dsn, const client::ConnectOpts& opts) { return impl_->Connect(dsn, opts); }
Error Reindexer::Stop() { return impl_->Stop(); }
Error Reindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef, ctx_); }
Error Reindexer::OpenNamespace(string_view nsName, const StorageOpts& storage) { return impl_->OpenNamespace(nsName, ctx_, storage); }
Error Reindexer::DropNamespace(string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error Reindexer::CloseNamespace(string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error Reindexer::TruncateNamespace(string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error Reindexer::Insert(string_view nsName, Item& item) { return impl_->Insert(nsName, item, ctx_); }
Error Reindexer::Update(string_view nsName, Item& item) { return impl_->Update(nsName, item, ctx_); }
Error Reindexer::Update(const Query& q, QueryResults& result) { return impl_->Update(q, result, ctx_); }
Error Reindexer::Upsert(string_view nsName, Item& item) { return impl_->Upsert(nsName, item, ctx_); }
Error Reindexer::Delete(string_view nsName, Item& item) { return impl_->Delete(nsName, item, ctx_); }
Item Reindexer::NewItem(string_view nsName) { return impl_->NewItem(nsName); }
Error Reindexer::GetMeta(string_view nsName, const string& key, string& data) { return impl_->GetMeta(nsName, key, data, ctx_); }
Error Reindexer::PutMeta(string_view nsName, const string& key, const string_view& data) { return impl_->PutMeta(nsName, key, data, ctx_); }
Error Reindexer::EnumMeta(string_view nsName, vector<string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error Reindexer::Select(string_view query, QueryResults& result) { return impl_->Select(query, result, ctx_, nullptr); }
Error Reindexer::Select(const Query& q, QueryResults& result) { return impl_->Select(q, result, ctx_, nullptr); }
Error Reindexer::Commit(string_view nsName) { return impl_->Commit(nsName); }
Error Reindexer::AddIndex(string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error Reindexer::UpdateIndex(string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error Reindexer::DropIndex(string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) { return impl_->EnumNamespaces(defs, bEnumAll, ctx_); }
Error Reindexer::EnumDatabases(vector<string>& dbList) { return impl_->EnumDatabases(dbList, ctx_); }
Error Reindexer::SubscribeUpdates(IUpdatesObserver* observer, bool subscribe) { return impl_->SubscribeUpdates(observer, subscribe); }
Error Reindexer::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string>& suggests) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggests);
}
Error Reindexer::Status() { return impl_->Status(); }

}  // namespace client
}  // namespace reindexer
