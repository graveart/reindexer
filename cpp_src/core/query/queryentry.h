#pragma once

#include <climits>
#include <string>
#include <vector>
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"
#include "querytree.h"

namespace reindexer {

class Query;
using std::string;
using std::vector;

struct QueryEntry {
	const static int kNoJoins = -1;

	QueryEntry(int joinIdx) : joinIndex(joinIdx) {}
	QueryEntry(CondType cond, const string &idx, int idxN, bool dist = false) : index(idx), idxNo(idxN), condition(cond), distinct(dist) {}
	QueryEntry() = default;

	bool operator==(const QueryEntry &) const;
	bool operator!=(const QueryEntry &) const;

	string index;
	int idxNo = IndexValueType::NotSet;
	CondType condition = CondType::CondAny;
	bool distinct = false;
	VariantArray values;
	int joinIndex = kNoJoins;

	string Dump() const;
};

struct EqualPosition : public h_vector<unsigned, 2> {};

class JsonBuilder;
extern template bool QueryTree<QueryEntry, 4>::Leaf::IsEqual(const Node &) const;

class QueryEntries : public QueryTree<QueryEntry, 4> {
public:
	bool IsEntry(size_t i) const { return IsValue(i); }
	void ForeachEntry(const std::function<void(const QueryEntry &, OpType)> &func) const { ForeachValue(func); }

	template <typename T>
	std::pair<unsigned, EqualPosition> DetermineEqualPositionIndexes(const T &fields) const;
	template <typename T>
	EqualPosition DetermineEqualPositionIndexes(unsigned start, const T &fields) const;
	void ToDsl(const Query &parentQuery, JsonBuilder &builder) const { return toDsl(cbegin(), cend(), parentQuery, builder); }
	void WriteSQLWhere(const Query &parentQuery, WrSerializer &, bool stripArgs) const;
	void Serialize(WrSerializer &ser) const { serialize(cbegin(), cend(), ser); }

private:
	static void toDsl(const_iterator it, const_iterator to, const Query &parentQuery, JsonBuilder &);
	static void writeSQL(const Query &parentQuery, const_iterator from, const_iterator to, WrSerializer &, bool stripArgs);
	static void serialize(const_iterator it, const_iterator to, WrSerializer &);
};

extern template EqualPosition QueryEntries::DetermineEqualPositionIndexes<vector<string>>(unsigned start,
																						  const vector<string> &fields) const;
extern template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<vector<string>>(
	const vector<string> &fields) const;
extern template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<h_vector<string, 4>>(
	const h_vector<string, 4> &fields) const;
extern template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<std::initializer_list<string>>(
	const std::initializer_list<string> &fields) const;

struct UpdateEntry {
	UpdateEntry() {}
	UpdateEntry(const string &c, const VariantArray &v) : column(c), values(v) {}
	bool operator==(const UpdateEntry &) const;
	bool operator!=(const UpdateEntry &) const;
	string column;
	VariantArray values;
	bool isExpression = false;
};

struct QueryJoinEntry {
	bool operator==(const QueryJoinEntry &) const;
	OpType op_ = OpAnd;
	CondType condition_ = CondEq;
	string index_;
	string joinIndex_;
	int idxNo = -1;
};

struct SortingEntry {
	SortingEntry() {}
	SortingEntry(const string &c, bool d) : column(c), desc(d) {}
	bool operator==(const SortingEntry &) const;
	bool operator!=(const SortingEntry &) const;
	string column;
	bool desc = false;
	int index = IndexValueType::NotSet;
};

struct SortingEntries : public h_vector<SortingEntry, 1> {};

struct AggregateEntry {
	AggregateEntry() = default;
	AggregateEntry(AggType type, const h_vector<string, 1> &fields, unsigned limit, unsigned offset)
		: type_(type), fields_(fields), limit_(limit), offset_(offset) {}
	bool operator==(const AggregateEntry &) const;
	bool operator!=(const AggregateEntry &) const;
	AggType type_;
	h_vector<string, 1> fields_;
	SortingEntries sortingEntries_;
	unsigned limit_ = UINT_MAX;
	unsigned offset_ = 0;
};

}  // namespace reindexer
