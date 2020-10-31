#pragma once

#include <memory.h>
#include "core/index/payload_map.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/fieldsset.h"
#include "estl/fast_hash_set.h"
#include "estl/intrusive_ptr.h"
#include "tools/string_regexp_functions.h"

namespace reindexer {

struct ComparatorVars {
	ComparatorVars(CondType cond, KeyValueType type, bool isArray, PayloadType payloadType, const FieldsSet &fields, void *rawData,
				   const CollateOpts &collateOpts)
		: cond_(cond),
		  type_(type),
		  isArray_(isArray),
		  rawData_(reinterpret_cast<uint8_t *>(rawData)),
		  collateOpts_(collateOpts),
		  payloadType_(payloadType),
		  fields_(fields) {}
	ComparatorVars() {}

	CondType cond_ = CondEq;
	KeyValueType type_ = KeyValueUndefined;
	bool isArray_ = false;
	unsigned offset_ = 0;
	unsigned sizeof_ = 0;
	uint8_t *rawData_ = nullptr;
	CollateOpts collateOpts_;
	PayloadType payloadType_;
	FieldsSet fields_;
};

template <class T>
class ComparatorImpl {
public:
	ComparatorImpl(bool distinct = false) : distS_(distinct ? new intrusive_atomic_rc_wrapper<fast_hash_set<T>> : nullptr) {}

	void SetValues(CondType cond, const VariantArray &values) {
		if (cond == CondSet) valuesS_.reset(new intrusive_atomic_rc_wrapper<fast_hash_set<T>>());

		for (Variant key : values) {
			if (key.Type() == KeyValueString && !is_number(static_cast<p_string>(key))) {
				addValue(cond, T());
			} else {
				key.convert(type());
				addValue(cond, static_cast<T>(key));
			}
		}
	}

	inline bool Compare2(CondType cond, T lhs) {
		T rhs = values_[0];
		switch (cond) {
			case CondEq:
				return lhs == rhs;
			case CondGe:
				return lhs >= rhs;
			case CondLe:
				return lhs <= rhs;
			case CondLt:
				return lhs < rhs;
			case CondGt:
				return lhs > rhs;
			case CondRange:
				return lhs >= rhs && lhs <= values_[1];
			case CondSet:
				return valuesS_->find(lhs) != valuesS_->end();
			case CondAny:
				return true;
			case CondEmpty:
			case CondLike:
				return false;
			default:
				abort();
		}
	}
	bool Compare(CondType cond, T lhs) {
		bool ret = Compare2(cond, lhs);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs) == distS_->end();
	}

	void ExcludeDistinct(T value) { distS_->emplace(value); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}

	h_vector<T, 1> values_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<fast_hash_set<T>>> valuesS_, distS_;

private:
	KeyValueType type() {
		if (std::is_same<T, int>::value) return KeyValueInt;
		if (std::is_same<T, bool>::value) return KeyValueBool;
		if (std::is_same<T, int64_t>::value) return KeyValueInt64;
		if (std::is_same<T, double>::value) return KeyValueDouble;
		std::abort();
	}

	void addValue(CondType cond, T value) {
		if (cond == CondSet) {
			valuesS_->insert(value);
		} else {
			values_.push_back(value);
		}
	}
};

template <>
class ComparatorImpl<key_string> {
public:
	ComparatorImpl(bool distinct = false) : distS_(distinct ? new intrusive_atomic_rc_wrapper<fast_hash_set<key_string>> : nullptr) {}

	void SetValues(CondType cond, const VariantArray &values) {
		if (cond == CondSet) valuesS_.reset(new intrusive_atomic_rc_wrapper<fast_hash_set<key_string>>());

		for (Variant key : values) {
			key.convert(KeyValueString);
			addValue(cond, static_cast<key_string>(key));
		}
	}

	bool inline Compare2(CondType cond, p_string lhs, const CollateOpts &collateOpts) {
		auto rhs = cachedValueSV_;
		switch (cond) {
			case CondEq:
				return collateCompare(string_view(lhs), rhs, collateOpts) == 0;
			case CondGe:
				return collateCompare(string_view(lhs), rhs, collateOpts) >= 0;
			case CondLe:
				return collateCompare(string_view(lhs), rhs, collateOpts) <= 0;
			case CondLt:
				return collateCompare(string_view(lhs), rhs, collateOpts) < 0;
			case CondGt:
				return collateCompare(string_view(lhs), rhs, collateOpts) > 0;
			case CondRange:
				return collateCompare(string_view(lhs), rhs, collateOpts) >= 0 &&
					   collateCompare(string_view(lhs), string_view(*values_[1]), collateOpts) <= 0;
			case CondSet:
				// if (collateOpts.mode == CollateNone) return valuesS_->find(lhs) != valuesS_->end();
				for (auto it : *valuesS_) {
					if (!collateCompare(string_view(lhs), string_view(*it), collateOpts)) return true;
				}
				return false;
			case CondAny:
				return true;
			case CondEmpty:
				return false;
			case CondLike: {
				return matchLikePattern(string_view(lhs), rhs);
			}
			default:
				abort();
		}
	}
	bool Compare(CondType cond, p_string lhs, const CollateOpts &collateOpts) {
		bool ret = Compare2(cond, lhs, collateOpts);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs.getOrMakeKeyString()) == distS_->end();
	}

	void ExcludeDistinct(p_string value) { distS_->emplace(value.getOrMakeKeyString()); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}

	h_vector<key_string, 1> values_;
	string_view cachedValueSV_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<fast_hash_set<key_string>>> valuesS_, distS_;

private:
	void addValue(CondType cond, const key_string &value) {
		if (cond == CondSet) {
			valuesS_->emplace(value);
		} else {
			values_.push_back(value);
			if (values_.size() == 1) {
				cachedValueSV_ = string_view(*values_[0]);
			}
		}
	}
};

template <>
class ComparatorImpl<PayloadValue> {
public:
	ComparatorImpl() {}

	void SetValues(CondType cond, const VariantArray &values, const ComparatorVars &vars) {
		if (cond == CondSet)
			valuesSet_.reset(new intrusive_atomic_rc_wrapper<unordered_payload_set>(0, hash_composite(vars.payloadType_, vars.fields_),
																					equal_composite(vars.payloadType_, vars.fields_)));

		for (const Variant &kv : values) {
			addValue(cond, static_cast<const PayloadValue &>(kv));
		}
	}

	bool Compare(CondType cond, const PayloadValue &leftValue, const ComparatorVars &vars) {
		assert(!values_.empty() || !valuesSet_->empty());
		assert(vars.fields_.size() > 0);
		PayloadValue *rightValue(&values_[0]);
		ConstPayload lhs(vars.payloadType_, leftValue);
		switch (cond) {
			case CondEq:
				return (lhs.Compare(*rightValue, vars.fields_, vars.collateOpts_) == 0);
			case CondGe:
				return (lhs.Compare(*rightValue, vars.fields_, vars.collateOpts_) >= 0);
			case CondGt:
				return (lhs.Compare(*rightValue, vars.fields_, vars.collateOpts_) > 0);
			case CondLe:
				return (lhs.Compare(*rightValue, vars.fields_, vars.collateOpts_) <= 0);
			case CondLt:
				return (lhs.Compare(*rightValue, vars.fields_, vars.collateOpts_) < 0);
			case CondRange:
				return (lhs.Compare(*rightValue, vars.fields_, vars.collateOpts_) >= 0) &&
					   (lhs.Compare(values_[1], vars.fields_, vars.collateOpts_) <= 0);
			case CondSet:
				return valuesSet_->find(leftValue) != valuesSet_->end();
			case CondAny:
				return true;
			case CondEmpty:
			case CondLike:
				return false;
			default:
				abort();
		}
	}

	h_vector<PayloadValue, 1> values_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<unordered_payload_set>> valuesSet_;

private:
	void addValue(CondType cond, const PayloadValue &pv) {
		if (cond == CondSet) {
			valuesSet_->emplace(pv);
		} else {
			values_.push_back(pv);
		}
	}
};

template <>
class ComparatorImpl<Point> {
public:
	ComparatorImpl(bool distinct = false)
		: distS_(distinct ? new intrusive_atomic_rc_wrapper<fast_hash_set<Point>> : nullptr), rhs_{}, distance_{} {}

	void SetValues(const VariantArray &values) {
		if (values.size() != 2) throw Error(errQueryExec, "CondDWithin expects two arguments");
		if (values[0].Type() == KeyValueTuple) {
			rhs_ = values[0].As<Point>();
			distance_ = values[1].As<double>();
		} else {
			rhs_ = values[1].As<Point>();
			distance_ = values[0].As<double>();
		}
	}

	bool inline Compare2(Point lhs) const noexcept { return DWithin(lhs, rhs_, distance_); }
	bool Compare(Point lhs) {
		bool ret = Compare2(lhs);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs) == distS_->end();
	}

	void ExcludeDistinct(Point value) { distS_->emplace(value); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}

	intrusive_ptr<intrusive_atomic_rc_wrapper<fast_hash_set<Point>>> distS_;

private:
	Point rhs_;
	double distance_;
};

}  // namespace reindexer
