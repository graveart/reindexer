#include "core/queryresults/queryresults.h"
#include "core/cjson/baseencoder.h"
#include "core/itemimpl.h"
#include "tools/logger.h"

namespace reindexer {

struct QueryResults::Context {
	Context() {}
	Context(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet &fieldsFilter)
		: type_(type), tagsMatcher_(tagsMatcher), fieldsFilter_(fieldsFilter) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsSet fieldsFilter_;
};

static_assert(sizeof(QueryResults::Context) < QueryResults::kSizeofContext,
			  "QueryResults::kSizeofContext should >=  sizeof(QueryResults::Context)");

QueryResults::QueryResults(std::initializer_list<ItemRef> l) : items_(l), holdActivity_(false), noActivity_(0) {}
QueryResults::QueryResults(int /*flags*/) : holdActivity_(false), noActivity_(0){};
QueryResults::QueryResults(QueryResults &&obj)
	: joined_(std::move(obj.joined_)),
	  aggregationResults(std::move(obj.aggregationResults)),
	  totalCount(obj.totalCount),
	  haveProcent(obj.haveProcent),
	  nonCacheableData(obj.nonCacheableData),
	  ctxs(std::move(obj.ctxs)),
	  explainResults(std::move(obj.explainResults)),
	  lockedResults_(obj.lockedResults_),
	  items_(std::move(obj.items_)),
	  holdActivity_(obj.holdActivity_),
	  noActivity_(0) {
	if (holdActivity_) {
		new (&activityCtx_) RdxActivityContext(std::move(obj.activityCtx_));
		obj.activityCtx_.~RdxActivityContext();
		obj.holdActivity_ = false;
	}
}

QueryResults::QueryResults(const ItemRefVector::const_iterator &begin, const ItemRefVector::const_iterator &end)
	: items_(begin, end), holdActivity_(false), noActivity_(0) {}
QueryResults &QueryResults::operator=(QueryResults &&obj) noexcept {
	if (this != &obj) {
		unlockResults();
		items_ = std::move(obj.items_);
		assert(!obj.items_.size());
		joined_ = std::move(obj.joined_);
		aggregationResults = std::move(obj.aggregationResults);
		totalCount = std::move(obj.totalCount);
		haveProcent = std::move(obj.haveProcent);
		ctxs = std::move(obj.ctxs);
		nonCacheableData = std::move(obj.nonCacheableData);
		lockedResults_ = std::move(obj.lockedResults_);
		explainResults = std::move(obj.explainResults);
		if (holdActivity_) activityCtx_.~RdxActivityContext();
		holdActivity_ = obj.holdActivity_;
		if (holdActivity_) {
			new (&activityCtx_) RdxActivityContext(std::move(obj.activityCtx_));
			obj.activityCtx_.~RdxActivityContext();
			obj.holdActivity_ = false;
		}
		obj.lockedResults_ = false;
	}
	return *this;
}

QueryResults::~QueryResults() {
	unlockResults();
	if (holdActivity_) activityCtx_.~RdxActivityContext();
}

void QueryResults::Clear() { *this = QueryResults(); }

void QueryResults::Erase(ItemRefVector::iterator start, ItemRefVector::iterator finish) {
	assert(!lockedResults_);
	items_.erase(start, finish);
}

void QueryResults::lockItem(ItemRef &itemref, size_t joinedNs, bool lock) {
	if (!itemref.value.IsFree() && !itemref.raw) {
		assert(ctxs.size() > joinedNs);
		Payload pl(ctxs[joinedNs].type_, itemref.value);
		if (lock)
			pl.AddRefStrings();
		else
			pl.ReleaseStrings();
	}
}

void QueryResults::lockResults() { lockResults(true); }
void QueryResults::unlockResults() { lockResults(false); }

void QueryResults::lockResults(bool lock) {
	if (!lock && !lockedResults_) return;
	if (lock) assert(!lockedResults_);
	for (size_t i = 0; i < items_.size(); ++i) {
		lockItem(items_[i], items_[i].nsid, lock);
		if (joined_.empty()) continue;
		Iterator itemIt{this, int(i), errOK};
		joins::ItemIterator joinIt = itemIt.GetJoinedItemsIterator();
		if (joinIt.getJoinedItemsCount() == 0) continue;
		size_t joinedNs = joined_.size();
		for (auto fieldIt = joinIt.begin(); fieldIt != joinIt.end(); ++fieldIt, ++joinedNs) {
			for (int j = 0; j < fieldIt.ItemsCount(); ++j) lockItem(fieldIt[j], joinedNs, lock);
		}
	}
	lockedResults_ = lock;
}

void QueryResults::Add(const ItemRef &i) {
	items_.push_back(i);

	if (!lockedResults_) return;

	if (!i.value.IsFree() && !i.raw) {
		assert(ctxs.size() > items_.back().nsid);
		Payload(ctxs[items_.back().nsid].type_, items_.back().value).AddRefStrings();
	}
}

void QueryResults::Add(const ItemRef &itemref, const PayloadType &pt) {
	items_.push_back(itemref);

	if (!lockedResults_) return;
	if (!itemref.value.IsFree() && !itemref.raw) {
		Payload(pt, items_.back().value).AddRefStrings();
	}
}

void QueryResults::Dump() const {
	string buf;
	for (size_t i = 0; i < items_.size(); ++i) {
		if (&items_[i] != &*items_.begin()) buf += ",";
		buf += std::to_string(items_[i].id);
		if (joined_.empty()) continue;
		Iterator itemIt{this, int(i), errOK};
		joins::ItemIterator joinIt = itemIt.GetJoinedItemsIterator();
		if (joinIt.getJoinedItemsCount() > 0) {
			buf += "[";
			for (auto fieldIt = joinIt.begin(); fieldIt != joinIt.end(); ++fieldIt) {
				if (fieldIt != joinIt.begin()) buf += ";";
				for (int j = 0; j < fieldIt.ItemsCount(); ++j) {
					if (j != 0) buf += ",";
					buf += std::to_string(fieldIt[j].id);
				}
			}
			buf += "]";
		}
	}

	logPrintf(LogInfo, "Query returned: [%s]; total=%d", buf, this->totalCount);
}

h_vector<string_view, 1> QueryResults::GetNamespaces() const {
	h_vector<string_view, 1> ret;
	ret.reserve(ctxs.size());
	for (auto &ctx : ctxs) ret.push_back(ctx.type_.Name());
	return ret;
}

class QueryResults::EncoderDatasourceWithJoins : public IEncoderDatasourceWithJoins {
public:
	EncoderDatasourceWithJoins(const joins::ItemIterator &joinedItemIt, const ContextsVector &ctxs)
		: joinedItemIt_(joinedItemIt), ctxs_(ctxs) {}
	~EncoderDatasourceWithJoins() {}

	size_t GetJoinedRowsCount() const final { return joinedItemIt_.getJoinedFieldsCount(); }
	size_t GetJoinedRowItemsCount(size_t rowId) const final {
		auto fieldIt = joinedItemIt_.at(rowId);
		return fieldIt.ItemsCount();
	}
	ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) const final {
		auto fieldIt = joinedItemIt_.at(rowid);
		const ItemRef &itemRef = fieldIt[plIndex];
		const Context &ctx = ctxs_[rowid + 1];
		return ConstPayload(ctx.type_, itemRef.value);
	}
	const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.tagsMatcher_;
	}
	virtual const FieldsSet &GetJoinedItemFieldsFilter(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.fieldsFilter_;
	}

	const string &GetJoinedItemNamespace(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.type_->Name();
	}

private:
	const joins::ItemIterator &joinedItemIt_;
	const ContextsVector &ctxs_;
};

void QueryResults::encodeJSON(int idx, WrSerializer &ser) const {
	auto &itemRef = items_[idx];
	assert(ctxs.size() > itemRef.nsid);
	auto &ctx = ctxs[itemRef.nsid];

	if (itemRef.value.IsFree()) {
		ser << "{}";
		return;
	}
	ConstPayload pl(ctx.type_, itemRef.value);
	JsonEncoder encoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

	JsonBuilder builder(ser, JsonBuilder::TypePlain);

	if (joined_.size() > 0) {
		joins::ItemIterator itemIt = (begin() + idx).GetJoinedItemsIterator();
		if (itemIt.getJoinedItemsCount() > 0) {
			EncoderDatasourceWithJoins ds(itemIt, ctxs);
			encoder.Encode(&pl, builder, &ds);
			return;
		}
	}
	encoder.Encode(&pl, builder);
}

Error QueryResults::Iterator::GetJSON(WrSerializer &ser, bool withHdrLen) {
	try {
		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			qr_->encodeJSON(idx_, ser);
		} else {
			qr_->encodeJSON(idx_, ser);
		}
	} catch (const Error &err) {
		err_ = err;
		return err;
	}
	return errOK;
}

Error QueryResults::Iterator::GetCJSON(WrSerializer &ser, bool withHdrLen) {
	try {
		auto &itemRef = qr_->items_[idx_];
		assert(qr_->ctxs.size() > itemRef.nsid);
		auto &ctx = qr_->ctxs[itemRef.nsid];

		if (itemRef.value.IsFree()) {
			return Error(errNotFound, "Item not found");
		}

		ConstPayload pl(ctx.type_, itemRef.value);
		CJsonBuilder builder(ser, CJsonBuilder::TypePlain);
		CJsonEncoder cjsonEncoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			cjsonEncoder.Encode(&pl, builder);
		} else {
			cjsonEncoder.Encode(&pl, builder);
		}
	} catch (const Error &err) {
		err_ = err;
		return err;
	}
	return errOK;
}

bool QueryResults::Iterator::IsRaw() const {
	auto &itemRef = qr_->items_[idx_];
	return itemRef.raw;
}
string_view QueryResults::Iterator::GetRaw() const {
	auto &itemRef = qr_->items_[idx_];
	assert(itemRef.raw);
	return string_view(reinterpret_cast<char *>(itemRef.value.Ptr()), itemRef.value.GetCapacity());
}

Item QueryResults::Iterator::GetItem() {
	auto &itemRef = qr_->items_[idx_];

	assert(qr_->ctxs.size() > itemRef.nsid);
	auto &ctx = qr_->ctxs[itemRef.nsid];

	if (itemRef.value.IsFree()) {
		return Item(Error(errNotFound, "Item not found"));
	}

	PayloadValue v(itemRef.value);

	auto item = Item(new ItemImpl(ctx.type_, v, ctx.tagsMatcher_));
	item.setID(itemRef.id);
	return item;
}

joins::ItemIterator QueryResults::Iterator::GetJoinedItemsIterator() {
	static joins::NamespaceResults empty;
	static joins::ItemIterator ret(&empty, 0);
	auto &itemRef = qr_->items_[idx_];
	if (itemRef.nsid >= qr_->joined_.size()) return ret;
	return joins::ItemIterator(&qr_->joined_[itemRef.nsid], itemRef.id);
}

QueryResults::Iterator &QueryResults::Iterator::operator++() {
	idx_++;
	return *this;
}
QueryResults::Iterator &QueryResults::Iterator::operator+(int val) {
	idx_ += val;
	return *this;
}

bool QueryResults::Iterator::operator!=(const Iterator &other) const { return idx_ != other.idx_; }
bool QueryResults::Iterator::operator==(const Iterator &other) const { return idx_ == other.idx_; }

void QueryResults::AddItem(Item &item, bool withData) {
	auto ritem = item.impl_;
	if (item.GetID() != -1) {
		if (ctxs.empty()) ctxs.push_back(Context(ritem->Type(), ritem->tagsMatcher(), FieldsSet()));
		Add(ItemRef(item.GetID(), withData ? ritem->RealValue() : PayloadValue()));
		if (withData) {
			lockResults();
		}
	}
}

const TagsMatcher &QueryResults::getTagsMatcher(int nsid) const {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].tagsMatcher_;
}

const PayloadType &QueryResults::getPayloadType(int nsid) const {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].type_;
}

const FieldsSet &QueryResults::getFieldsFilter(int nsid) const {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].fieldsFilter_;
}

TagsMatcher &QueryResults::getTagsMatcher(int nsid) {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].tagsMatcher_;
}

PayloadType &QueryResults::getPayloadType(int nsid) {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].type_;
}

int QueryResults::getMergedNSCount() const { return ctxs.size(); }

void QueryResults::addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const FieldsSet &filter) {
	if (filter.getTagsPathsLength()) nonCacheableData = true;

	ctxs.push_back(Context(type, tagsMatcher, filter));
}

}  // namespace reindexer
