#pragma once

#include <cmath>
#include <limits>
#include <map>
#include <mutex>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include "core/nsselecter/sortexpression.h"
#include "reindexer_api.h"
#include "tools/string_regexp_functions.h"
#include "tools/stringstools.h"

using std::unordered_map;
using std::unordered_set;
using std::numeric_limits;
using std::to_string;
using reindexer::VariantArray;

class QueriesApi : public ReindexerApi {
public:
	void SetUp() override {
		indexesOptions = {
			{kFieldNameId, IndexOpts()},
			{kFieldNameGenre, IndexOpts()},
			{kFieldNameYear, IndexOpts()},
			{kFieldNamePackages, IndexOpts().Array()},
			{kFieldNameName, IndexOpts()},
			{kFieldNameCountries, IndexOpts().Array()},
			{kFieldNameAge, IndexOpts()},
			{kFieldNameDescription, IndexOpts()},
			{kFieldNameRate, IndexOpts()},
			{kFieldNameIsDeleted, IndexOpts()},
			{kFieldNameActor, IndexOpts().SetCollateMode(CollateUTF8)},
			{kFieldNamePriceId, IndexOpts().Array()},
			{kFieldNameLocation, IndexOpts().SetCollateMode(CollateNone)},
			{kFieldNameEndTime, IndexOpts()},
			{kFieldNameStartTime, IndexOpts()},
			{kFieldNamePhone, IndexOpts()},
			{kFieldNameBtreeIdsets, IndexOpts()},
			{kFieldNameTemp, IndexOpts().SetCollateMode(CollateASCII)},
			{kFieldNameNumeric, IndexOpts().SetCollateMode(CollateUTF8)},
			{string(kFieldNameId + compositePlus + kFieldNameTemp), IndexOpts().PK()},
			{string(kFieldNameAge + compositePlus + kFieldNameGenre), IndexOpts()},
		};

		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(default_namespace,
							   {
								   IndexDeclaration{kFieldNameId, "hash", "int", indexesOptions[kFieldNameId], 0},
								   IndexDeclaration{kFieldNameGenre, "tree", "int", indexesOptions[kFieldNameGenre], 0},
								   IndexDeclaration{kFieldNameYear, "tree", "int", indexesOptions[kFieldNameYear], 0},
								   IndexDeclaration{kFieldNamePackages, "hash", "int", indexesOptions[kFieldNamePackages], 0},
								   IndexDeclaration{kFieldNameName, "tree", "string", indexesOptions[kFieldNameName], 0},
								   IndexDeclaration{kFieldNameCountries, "tree", "string", indexesOptions[kFieldNameCountries], 0},
								   IndexDeclaration{kFieldNameAge, "hash", "int", indexesOptions[kFieldNameAge], 0},
								   IndexDeclaration{kFieldNameDescription, "fuzzytext", "string", indexesOptions[kFieldNameDescription], 0},
								   IndexDeclaration{kFieldNameRate, "tree", "double", indexesOptions[kFieldNameRate], 0},
								   IndexDeclaration{kFieldNameIsDeleted, "-", "bool", indexesOptions[kFieldNameIsDeleted], 0},
								   IndexDeclaration{kFieldNameActor, "tree", "string", indexesOptions[kFieldNameActor], 0},
								   IndexDeclaration{kFieldNamePriceId, "hash", "int", indexesOptions[kFieldNamePriceId], 0},
								   IndexDeclaration{kFieldNameLocation, "tree", "string", indexesOptions[kFieldNameLocation], 0},
								   IndexDeclaration{kFieldNameEndTime, "hash", "int", indexesOptions[kFieldNameEndTime], 0},
								   IndexDeclaration{kFieldNameStartTime, "tree", "int", indexesOptions[kFieldNameStartTime], 0},
								   IndexDeclaration{kFieldNameBtreeIdsets, "hash", "int", indexesOptions[kFieldNameBtreeIdsets], 0},
								   IndexDeclaration{kFieldNameTemp, "tree", "string", indexesOptions[kFieldNameTemp], 0},
								   IndexDeclaration{kFieldNameNumeric, "tree", "string", indexesOptions[kFieldNameNumeric], 0},
								   IndexDeclaration{string(kFieldNameId + compositePlus + kFieldNameTemp).c_str(), "tree", "composite",
													indexesOptions[kFieldNameId + compositePlus + kFieldNameTemp], 0},
								   IndexDeclaration{string(kFieldNameAge + compositePlus + kFieldNameGenre).c_str(), "hash", "composite",
													indexesOptions[kFieldNameAge + compositePlus + kFieldNameGenre], 0},
							   });
		defaultNsPks.push_back(kFieldNameId);
		defaultNsPks.push_back(kFieldNameTemp);

		err = rt.reindexer->OpenNamespace(testSimpleNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(testSimpleNs, {
												 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
												 IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0},
												 IndexDeclaration{kFieldNameName, "hash", "string", IndexOpts(), 0},
												 IndexDeclaration{kFieldNamePhone, "hash", "string", IndexOpts(), 0},
											 });
		simpleTestNsPks.push_back(kFieldNameId);

		err = rt.reindexer->OpenNamespace(compositeIndexesNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			compositeIndexesNs,
			{IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameTitle, "text", "string", IndexOpts(), 0},
			 IndexDeclaration{kFieldNamePages, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNamePrice, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameName, "text", "string", IndexOpts(), 0},
			 IndexDeclaration{kCompositeFieldPricePages.c_str(), "hash", "composite", IndexOpts(), 0},
			 IndexDeclaration{kCompositeFieldTitleName.c_str(), "tree", "composite", IndexOpts(), 0},
			 IndexDeclaration{(string(kFieldNameBookid) + "+" + kFieldNameBookid2).c_str(), "hash", "composite", IndexOpts().PK(), 0}});

		compositeIndexesNsPks.push_back(kFieldNameBookid);
		compositeIndexesNsPks.push_back(kFieldNameBookid2);

		err = rt.reindexer->OpenNamespace(comparatorsNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			comparatorsNs,
			{IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnInt, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnInt64, "hash", "int64", IndexOpts().PK(), 0},
			 IndexDeclaration{kFieldNameColumnDouble, "tree", "double", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnString, "-", "string", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnFullText, "text", "string", IndexOpts().SetConfig(R"xxx({"stemmers":[]})xxx"), 0},
			 IndexDeclaration{kFieldNameColumnStringNumeric, "-", "string", IndexOpts().SetCollateMode(CollateNumeric), 0}});
		comparatorsNsPks.push_back(kFieldNameColumnInt64);

		err = rt.reindexer->OpenNamespace(forcedSortOffsetNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(forcedSortOffsetNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
													IndexDeclaration{kFieldNameColumnHash, "hash", "int", IndexOpts(), 0},
													IndexDeclaration{kFieldNameColumnTree, "tree", "int", IndexOpts(), 0}});
		forcedSortOffsetNsPks.push_back(kFieldNameId);
	}

	template <typename... T>
	bool ExecuteAndVerify(const string& ns, const Query& query, T... args) {
		reindexer::QueryResults qr;
		const_cast<Query&>(query).Explain();
		Error err = rt.reindexer->Select(query, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		if (err.ok()) {
			Verify(ns, qr, query);
		}
		Verify(qr, args...);
		return err.ok();
	}

	bool ExecuteAndVerifyWithSql(const string& ns, const Query& query) {
		if (ExecuteAndVerify(ns, query)) {
			Query queryFromSql;
			queryFromSql.FromSQL(query.GetSQL());
			return ExecuteAndVerify(ns, queryFromSql);
		}
		return false;
	}

	static double CalculateSortExpression(reindexer::SortExpression::const_iterator begin, reindexer::SortExpression::const_iterator end,
										  Item& item) {
		double result = 0.0;
		assert(begin != end);
		assert(begin->Op.op == OpPlus);
		for (auto it = begin; it != end; ++it) {
			double value = 0.0;
			if (it->IsLeaf()) {
				const auto& sortExprValue = it->Value();
				switch (sortExprValue.type) {
					case reindexer::SortExpressionValue::Value:
						value = sortExprValue.value;
						break;
					case reindexer::SortExpressionValue::Index:
						value = item[sortExprValue.column].As<double>();
						break;
					case reindexer::SortExpressionValue::Rank:
						assert(0);
				}
			} else {
				value = CalculateSortExpression(it.cbegin(), it.cend(), item);
			}
			if (it->Op.negative) value = -value;
			switch (it->Op.op) {
				case OpPlus:
					result += value;
					break;
				case OpMinus:
					result -= value;
					break;
				case OpMult:
					result *= value;
					break;
				case OpDiv:
					assert(value != 0.0);
					result /= value;
					break;
			}
		}
		return result;
	}

	void Verify(const QueryResults&) {}

	template <typename... T>
	void Verify(const QueryResults& qr, const char* fieldName, const vector<Variant> expectedValues, T... args) {
		reindexer::WrSerializer ser;
		if (qr.Count() != expectedValues.size()) {
			ser << "Sizes different: expected size " << expectedValues.size() << ", obtained size " << qr.Count() << '\n';
		} else {
			for (size_t i = 0; i < expectedValues.size(); ++i) {
				Item item(qr[i].GetItem());
				const Variant fieldValue = item[fieldName];
				if (fieldValue != expectedValues[i]) {
					ser << "Field " << fieldName << " of item " << i << " different: expected ";
					expectedValues[i].Dump(ser);
					ser << " obtained ";
					fieldValue.Dump(ser);
					ser << '\n';
				}
			}
		}
		if (ser.Len()) {
			ser << "\nExpected values:\n";
			for (size_t i = 0; i < expectedValues.size(); ++i) {
				if (i != 0) ser << ", ";
				expectedValues[i].Dump(ser);
			}
			ser << "\nObtained values:\n";
			for (size_t i = 0; i < qr.Count(); ++i) {
				if (i != 0) ser << ", ";
				Item item(qr[i].GetItem());
				const Variant fieldValue = item[fieldName];
				fieldValue.Dump(ser);
			}
			FAIL() << ser.Slice() << std::endl;
		}
		Verify(qr, args...);
	}

	void Verify(const string& ns, const QueryResults& qr, const Query& query) {
		unordered_set<string> pks;
		unordered_map<string, unordered_set<string>> distincts;

		VariantArray lastSortedColumnValues;
		lastSortedColumnValues.resize(query.sortingEntries_.size());

		size_t itemsCount = 0;
		for (size_t i = 0; i < qr.Count(); ++i) {
			Item itemr(qr[i].GetItem());

			auto pk = getPkString(itemr, ns);
			EXPECT_TRUE(pks.insert(pk).second) << "Duplicated primary key: " + pk;

			InsertedItemsByPk& insertedItemsByPk = insertedItems[ns];
			auto itInsertedItem = insertedItemsByPk.find(pk);
			EXPECT_TRUE(itInsertedItem != insertedItemsByPk.end()) << "Item with such PK has not been inserted yet: " + pk;
			if (itInsertedItem != insertedItemsByPk.end()) {
				Item& insertedItem = itInsertedItem->second;
				bool eq = (insertedItem.GetJSON() == itemr.GetJSON());
				EXPECT_TRUE(eq) << "Items' jsons are different! pk: " << pk << std::endl
								<< "expect json: " << insertedItem.GetJSON() << std::endl
								<< "got json: " << itemr.GetJSON() << std::endl
								<< "expect fields: " << PrintItem(insertedItem) << std::endl
								<< "got fields: " << PrintItem(itemr) << std::endl
								<< "explain: " << qr.GetExplainResults();
			}

			bool conditionsSatisfied = checkConditions(itemr, query.entries.cbegin(), query.entries.cend());
			if (conditionsSatisfied) ++itemsCount;
			EXPECT_TRUE(conditionsSatisfied) << "Item doesn't match conditions: " << itemr.GetJSON() << std::endl
											 << "explain: " << qr.GetExplainResults();
			if (!conditionsSatisfied) {
				TEST_COUT << query.GetSQL() << std::endl;
				PrintFailedQueryEntries(query.entries);
			}
			EXPECT_TRUE(checkDistincts(itemr, query, distincts)) << "Distinction check failed";

			std::vector<int> cmpRes(query.sortingEntries_.size());
			std::fill(cmpRes.begin(), cmpRes.end(), -1);

			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				const reindexer::SortingEntry& sortingEntry(query.sortingEntries_[j]);
				const auto sortExpr = reindexer::SortExpression::Parse(sortingEntry.expression);
				Variant sortedValue;
				if (sortExpr.JustByIndex()) {
					sortedValue = itemr[sortingEntry.expression];
				} else {
					sortedValue = Variant{CalculateSortExpression(sortExpr.cbegin(), sortExpr.cend(), itemr)};
				}
				if (lastSortedColumnValues[j].Type() != KeyValueNull) {
					bool needToVerify = true;
					for (int k = j - 1; k >= 0; --k) {
						if (cmpRes[k] != 0) {
							needToVerify = false;
							break;
						}
					}
					if (needToVerify) {
						if (j == 0 && !query.forcedSortOrder_.empty()) {
							const auto currValIt = std::find(query.forcedSortOrder_.cbegin(), query.forcedSortOrder_.cend(), sortedValue);
							const auto lastValIt =
								std::find(query.forcedSortOrder_.cbegin(), query.forcedSortOrder_.cend(), lastSortedColumnValues[0]);
							if (lastValIt < currValIt) {
								cmpRes[0] = -1;
							} else if (lastValIt > currValIt) {
								cmpRes[0] = 1;
							} else if (lastValIt == query.forcedSortOrder_.cend()) {
								cmpRes[0] = lastSortedColumnValues[0].Compare(sortedValue);
							} else {
								cmpRes[0] = 0;
							}
						} else {
							cmpRes[j] = lastSortedColumnValues[j].Compare(sortedValue);
						}
						bool sortOrderSatisfied =
							(sortingEntry.desc && cmpRes[j] >= 0) || (!sortingEntry.desc && cmpRes[j] <= 0) || (cmpRes[j] == 0);
						EXPECT_TRUE(sortOrderSatisfied) << "\nSort order is incorrect for column: " << sortingEntry.expression;
						if (!sortOrderSatisfied) {
							TEST_COUT << query.GetSQL() << std::endl;
							PrintFailedSortOrder(query, qr, i);
						}
					}
				}
				lastSortedColumnValues[j] = sortedValue;
			}
		}

		// Check non found items, to not match conditions

		// If query has limit and offset, skip verification
		if (query.start != 0 || query.count != UINT_MAX) return;

		// If query has distinct, skip verification
		bool haveDistinct = false;
		query.entries.ForEachEntry([&haveDistinct](const reindexer::QueryEntry& qe, OpType) {
			if (qe.distinct) haveDistinct = true;
		});
		if (haveDistinct) return;

		for (auto& insertedItem : insertedItems[ns]) {
			if (pks.find(insertedItem.first) != pks.end()) continue;
			bool conditionsSatisfied = checkConditions(insertedItem.second, query.entries.cbegin(), query.entries.cend());

			EXPECT_FALSE(conditionsSatisfied) << "Item match conditions (found " << qr.Count()
											  << " items), but not found: " << insertedItem.second.GetJSON() << std::endl
											  << "query:" << query.GetSQL() << std::endl
											  << "explain: " << qr.GetExplainResults() << std::endl;
		}

		const auto& aggResults = qr.GetAggregationResults();
		ASSERT_EQ(aggResults.size(), query.aggregations_.size());
		for (size_t i = 0; i < aggResults.size(); ++i) {
			EXPECT_EQ(aggResults[i].type, query.aggregations_[i].type_) << "i = " << i;
			ASSERT_EQ(aggResults[i].fields.size(), query.aggregations_[i].fields_.size()) << "i = " << i;
			for (size_t j = 0; j < aggResults[i].fields.size(); ++j) {
				EXPECT_EQ(aggResults[i].fields[j], query.aggregations_[i].fields_[j]) << "i = " << i << ", j = " << j;
			}
			EXPECT_LE(aggResults[i].facets.size(), query.aggregations_[i].limit_) << "i = " << i;
		}
	}

protected:
	const std::vector<string>& getNsPks(const string& ns) {
		if (ns == default_namespace) return defaultNsPks;
		if (ns == testSimpleNs) return simpleTestNsPks;
		if (ns == compositeIndexesNs) return compositeIndexesNsPks;
		if (ns == comparatorsNs) return comparatorsNsPks;
		if (ns == forcedSortOffsetNs) return forcedSortOffsetNsPks;
		std::abort();
	}

	string getPkString(reindexer::Item& item, const string& ns) {
		string ret;
		const vector<string>& pkFields(getNsPks(ns));
		for (const string& field : pkFields) {
			ret += item[field].As<string>() + "#";
		}
		return ret;
	}

	bool checkConditions(reindexer::Item& item, reindexer::QueryEntries::const_iterator it, reindexer::QueryEntries::const_iterator to) {
		bool result = true;
		for (; it != to; ++it) {
			bool iterationResult = true;
			if (it->IsLeaf()) {
				if (it->Value().distinct) continue;
				iterationResult = checkCondition(item, it->Value());
			} else {
				iterationResult = checkConditions(item, it.cbegin(), it.cend());
			}
			switch (it->Op) {
				case OpNot:
					if (!result) return false;
					result = !iterationResult;
					break;
				case OpAnd:
					if (!result) return false;
					result = iterationResult;
					break;
				case OpOr:
					result = iterationResult || result;
					break;
			}
		}
		return result;
	}

	bool isIndexComposite(Item& item, const QueryEntry& qentry) {
		if (qentry.idxNo >= item.NumFields()) return true;
		return (qentry.values[0].Type() == KeyValueComposite || qentry.values[0].Type() == KeyValueTuple);
	}

	bool isLikeSqlPattern(reindexer::string_view str, reindexer::string_view pattern) {
		return std::regex_match(string(str), std::regex{reindexer::sqlLikePattern2ECMAScript(string(pattern))});
	}

	bool compareValues(CondType condition, Variant key, const VariantArray& values, const CollateOpts& opts) {
		bool result = false;
		try {
			if (values.size()) key.convert(values[0].Type());
		} catch (const Error& err) {
			return false;
		}
		switch (condition) {
			case CondEq:
				result = (key.Compare(values[0], opts) == 0);
				break;
			case CondGe:
				result = (key.Compare(values[0], opts) >= 0);
				break;
			case CondGt:
				result = (key.Compare(values[0], opts) > 0);
				break;
			case CondLt:
				result = (key.Compare(values[0], opts) < 0);
				break;
			case CondLe:
				result = (key.Compare(values[0], opts) <= 0);
				break;
			case CondRange:
				result = (key.Compare(values[0], opts) >= 0) && (key.Compare(values[1], opts) <= 0);
				break;
			case CondSet:
				for (const Variant& kv : values) {
					result = (key.Compare(kv, opts) == 0);
					if (result) break;
				}
				break;
			case CondLike:
				if (key.Type() != KeyValueString) {
					return false;
				}
				return isLikeSqlPattern(*static_cast<reindexer::key_string>(key.convert(KeyValueString)),
										*static_cast<reindexer::key_string>(values[0].convert(KeyValueString)));
			default:
				std::abort();
		}
		return result;
	}

	VariantArray getValues(Item& item, const std::vector<string>& indexes) {
		VariantArray kvalues;
		for (const string& idxName : indexes) {
			kvalues.push_back(item[idxName].operator Variant());
		}
		return kvalues;
	}

	int compareCompositeValues(const VariantArray& indexesValues, const Variant& keyValue, const CollateOpts& opts) {
		VariantArray compositeValues = keyValue.getCompositeValues();
		EXPECT_TRUE(indexesValues.size() == compositeValues.size());

		int cmpRes = 0;
		for (size_t i = 0; i < indexesValues.size() && (cmpRes == 0); ++i) {
			compositeValues[i].convert(indexesValues[i].Type());
			cmpRes = indexesValues[i].Compare(compositeValues[i], opts);
		}

		return cmpRes;
	}

	bool checkCompositeValues(Item& item, const QueryEntry& qentry, const CollateOpts& opts) {
		vector<string> subIndexes;
		reindexer::split(qentry.index, "+", true, subIndexes);

		VariantArray indexesValues = getValues(item, subIndexes);
		const VariantArray& keyValues = qentry.values;

		switch (qentry.condition) {
			case CondEmpty:
				return indexesValues.size() == 0;
			case CondAny:
				return indexesValues.size() > 0;
			default:
				break;
		}

		bool result = false;
		switch (qentry.condition) {
			case CondEq:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) == 0);
				break;
			case CondGe:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) >= 0);
				break;
			case CondGt:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) > 0);
				break;
			case CondLt:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) < 0);
				break;
			case CondLe:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) <= 0);
				break;
			case CondRange:
				EXPECT_TRUE(keyValues.size() == 2);
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) >= 0) &&
						 (compareCompositeValues(indexesValues, keyValues[1], opts) <= 0);
				break;
			case CondSet:
				for (const Variant& kv : keyValues) {
					result = (compareCompositeValues(indexesValues, kv, opts) == 0);
					if (result) break;
				}
				break;
			default:
				std::abort();
		}

		return result;
	}

	bool checkCondition(Item& item, const QueryEntry& qentry) {
		EXPECT_TRUE(item.NumFields() > 0);

		bool result = false;
		IndexOpts& opts = indexesOptions[qentry.index];

		if (isIndexComposite(item, qentry)) {
			return checkCompositeValues(item, qentry, opts.collateOpts_);
		} else {
			VariantArray fieldValues = item[qentry.index];
			switch (qentry.condition) {
				case CondEmpty:
					return fieldValues.size() == 0;
				case CondAny:
					return fieldValues.size() > 0;
				default:
					break;
			}
			for (const Variant& fieldValue : fieldValues) {
				result = compareValues(qentry.condition, fieldValue, qentry.values, opts.collateOpts_);
				if (result) break;
			}
		}

		return result;
	}

	bool checkDistincts(reindexer::Item& item, const Query& qr, unordered_map<string, unordered_set<string>>& distincts) {
		bool result = true;
		// check only on root level
		for (auto it = qr.entries.cbegin(); it != qr.entries.cend(); ++it) {
			if (!it->IsLeaf()) continue;
			const QueryEntry& qentry = it->Value();
			if (!qentry.distinct) continue;

			reindexer::VariantArray fieldValue = item[qentry.index];

			EXPECT_TRUE(fieldValue.size() == 1) << "Distinct field's size cannot be > 1";

			unordered_set<string>& values = distincts[qentry.index];
			Variant keyValue(fieldValue[0]);
			bool inserted = values.insert(keyValue.As<string>()).second;
			EXPECT_TRUE(inserted) << "Duplicate distinct item for index: " << keyValue.As<string>() << ", " << std::to_string(qentry.idxNo);
			result &= inserted;
		}
		return result;
	}

	void FillCompositeIndexesNamespace(size_t since, size_t till) {
		for (size_t i = since; i < till; ++i) {
			int idValue(static_cast<int>(i));
			Item item = NewItem(compositeIndexesNs);
			item[this->kFieldNameBookid] = idValue;
			item[this->kFieldNameBookid2] = idValue + 77777;
			item[this->kFieldNameTitle] = kFieldNameTitle + RandString();
			item[this->kFieldNamePages] = rand() % 1000 + 10;
			item[this->kFieldNamePrice] = rand() % 1000 + 150;
			item[this->kFieldNameName] = kFieldNameName + RandString();

			Upsert(compositeIndexesNs, item);
			Commit(compositeIndexesNs);

			string pkString = getPkString(item, compositeIndexesNs);
			insertedItems[compositeIndexesNs][pkString] = std::move(item);
		}

		Item lastItem = NewItem(compositeIndexesNs);
		lastItem[this->kFieldNameBookid] = 300;
		lastItem[this->kFieldNameBookid2] = 3000;
		lastItem[this->kFieldNameTitle] = "test book1 title";
		lastItem[this->kFieldNamePages] = 88888;
		lastItem[this->kFieldNamePrice] = 77777;
		lastItem[this->kFieldNameName] = "test book1 name";
		Upsert(compositeIndexesNs, lastItem);
		Commit(compositeIndexesNs);

		string pkString = getPkString(lastItem, compositeIndexesNs);
		insertedItems[compositeIndexesNs][pkString] = std::move(lastItem);
	}

	void FillForcedSortNamespace() {
		forcedSortOffsetValues.clear();
		forcedSortOffsetValues.reserve(forcedSortOffsetNsSize);
		for (size_t i = 0; i < forcedSortOffsetNsSize; ++i) {
			Item item = NewItem(forcedSortOffsetNs);
			item[kFieldNameId] = static_cast<int>(i);
			forcedSortOffsetValues.emplace_back(rand() % forcedSortOffsetMaxValue, rand() % forcedSortOffsetMaxValue);
			item[kFieldNameColumnHash] = forcedSortOffsetValues.back().first;
			item[kFieldNameColumnTree] = forcedSortOffsetValues.back().second;
			Upsert(forcedSortOffsetNs, item);
			string pkString = getPkString(item, forcedSortOffsetNs);
			insertedItems[forcedSortOffsetNs][pkString] = std::move(item);
		}
		Commit(forcedSortOffsetNs);
	}

	void FillTestSimpleNamespace() {
		Item item1 = NewItem(testSimpleNs);
		item1[kFieldNameId] = 1;
		item1[kFieldNameYear] = 2002;
		item1[kFieldNameName] = "SSS";
		Upsert(testSimpleNs, item1);

		string pkString = getPkString(item1, testSimpleNs);
		insertedItems[testSimpleNs].emplace(pkString, std::move(item1));

		Item item2 = NewItem(testSimpleNs);
		item2[kFieldNameId] = 2;
		item2[kFieldNameYear] = 1989;
		item2[kFieldNameName] = "MMM";
		Upsert(testSimpleNs, item2);

		pkString = getPkString(item2, testSimpleNs);
		insertedItems[testSimpleNs].emplace(pkString, std::move(item2));

		Commit(testSimpleNs);
	}

	enum Column { First, Second };

	vector<Variant> ForcedSortOffsetTestExpectedResults(size_t offset, size_t limit, bool desc, const vector<int>& forcedSortOrder,
														Column column) const {
		if (limit == 0 || offset >= forcedSortOffsetValues.size()) return {};
		vector<int> res;
		res.resize(forcedSortOffsetValues.size());
		std::transform(
			forcedSortOffsetValues.cbegin(), forcedSortOffsetValues.cend(), res.begin(),
			column == First ? [](const pair<int, int>& v) { return v.first; } : [](const pair<int, int>& v) { return v.second; });
		std::sort(
			res.begin(), res.end(), desc ? [](int lhs, int rhs) { return lhs > rhs; } : [](int lhs, int rhs) { return lhs < rhs; });
		const auto boundary = std::stable_partition(res.begin(), res.end(), [&forcedSortOrder, desc](int v) {
			return desc == (std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), v) == forcedSortOrder.cend());
		});
		if (desc) {
			std::sort(boundary, res.end(), [&forcedSortOrder](int lhs, int rhs) {
				return std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs) >
					   std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs);
			});
		} else {
			std::sort(res.begin(), boundary, [&forcedSortOrder](int lhs, int rhs) {
				return std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs) <
					   std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs);
			});
		}
		return {res.cbegin() + offset, (offset + limit >= res.size()) ? res.cend() : (res.begin() + offset + limit)};
	}

	pair<vector<Variant>, vector<Variant>> ForcedSortOffsetTestExpectedResults(size_t offset, size_t limit, bool desc1Column,
																			   bool desc2Column, const vector<int>& forcedSortOrder,
																			   Column firstSortColumn) {
		if (limit == 0 || offset >= forcedSortOffsetValues.size()) return {};
		if (firstSortColumn == First) {
			std::sort(forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(),
					  [desc1Column, desc2Column](pair<int, int> lhs, pair<int, int> rhs) {
						  return lhs.first == rhs.first ? (desc2Column ? (lhs.second > rhs.second) : (lhs.second < rhs.second))
														: (desc1Column ? (lhs.first > rhs.first) : (lhs.first < rhs.first));
					  });
			const auto boundary = std::stable_partition(
				forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(), [&forcedSortOrder, desc1Column](pair<int, int> v) {
					return desc1Column == (std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), v.first) == forcedSortOrder.cend());
				});
			std::sort(desc1Column ? boundary : forcedSortOffsetValues.begin(), desc1Column ? forcedSortOffsetValues.end() : boundary,
					  [&forcedSortOrder, desc1Column, desc2Column](pair<int, int> lhs, pair<int, int> rhs) {
						  const auto lhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs.first);
						  const auto rhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs.first);
						  if (lhsPos == rhsPos) {
							  return desc2Column ? lhs.second > rhs.second : lhs.second < rhs.second;
						  } else {
							  return desc1Column ? lhsPos > rhsPos : lhsPos < rhsPos;
						  }
					  });
		} else {
			std::sort(forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(),
					  [desc1Column, desc2Column](pair<int, int> lhs, pair<int, int> rhs) {
						  return lhs.second == rhs.second ? (desc1Column ? (lhs.first > rhs.first) : (lhs.first < rhs.first))
														  : (desc2Column ? (lhs.second > rhs.second) : (lhs.second < rhs.second));
					  });
			const auto boundary = std::stable_partition(
				forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(), [&forcedSortOrder, desc2Column](pair<int, int> v) {
					return desc2Column == (std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), v.second) == forcedSortOrder.cend());
				});
			std::sort(desc2Column ? boundary : forcedSortOffsetValues.begin(), desc2Column ? forcedSortOffsetValues.end() : boundary,
					  [&forcedSortOrder, desc1Column, desc2Column](pair<int, int> lhs, pair<int, int> rhs) {
						  const auto lhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs.second);
						  const auto rhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs.second);
						  if (lhsPos == rhsPos) {
							  return desc1Column ? lhs.first > rhs.first : lhs.first < rhs.first;
						  } else {
							  return desc2Column ? lhsPos > rhsPos : lhsPos < rhsPos;
						  }
					  });
		}
		vector<Variant> resFirstColumn, resSecondColumn;
		resFirstColumn.resize(std::min(limit, forcedSortOffsetValues.size() - offset));
		resSecondColumn.resize(std::min(limit, forcedSortOffsetValues.size() - offset));
		const bool byLimit = limit + offset < forcedSortOffsetValues.size();
		std::transform(forcedSortOffsetValues.cbegin() + offset,
					   byLimit ? (forcedSortOffsetValues.cbegin() + offset + limit) : forcedSortOffsetValues.cend(), resFirstColumn.begin(),
					   [](const pair<int, int>& v) { return Variant(v.first); });
		std::transform(forcedSortOffsetValues.cbegin() + offset,
					   byLimit ? (forcedSortOffsetValues.cbegin() + offset + limit) : forcedSortOffsetValues.cend(),
					   resSecondColumn.begin(), [](const pair<int, int>& v) { return Variant(v.second); });
		return std::make_pair(std::move(resFirstColumn), std::move(resSecondColumn));
	}

	void FillComparatorsNamespace() {
		for (size_t i = 0; i < 1000; ++i) {
			Item item(rt.reindexer->NewItem(comparatorsNs));
			item[kFieldNameId] = static_cast<int>(i);
			item[kFieldNameColumnInt] = rand();
			item[kFieldNameColumnInt64] = static_cast<int64_t>(rand());
			item[kFieldNameColumnDouble] = static_cast<double>(rand()) / RAND_MAX;
			item[kFieldNameColumnString] = RandString();
			item[kFieldNameColumnStringNumeric] = std::to_string(i);
			item[kFieldNameColumnFullText] = RandString();

			Upsert(comparatorsNs, item);

			string pkString = getPkString(item, comparatorsNs);
			insertedItems[comparatorsNs][pkString] = std::move(item);
		}

		Commit(comparatorsNs);
	}

	void FillDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);

			string pkString = getPkString(item, default_namespace);
			insertedItems[default_namespace].emplace(pkString, std::move(item));
		}
		Commit(default_namespace);
	}

	void AddToDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = start; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);

			string pkString = getPkString(item, default_namespace);
		}
		Commit(default_namespace);
	}

	void FillDefaultNamespaceTransaction(int start, int count, int packagesCount) {
		auto tr = rt.reindexer->NewTransaction(default_namespace);

		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));

			string pkString = getPkString(item, default_namespace);

			tr.Insert(move(item));
		}
		QueryResults res;
		rt.reindexer->CommitTransaction(tr,res);
		Commit(default_namespace);
	}

	int GetcurrBtreeIdsetsValue(int id) {
		std::lock_guard<std::mutex> l(m_);
		if (id % 200) currBtreeIdsetsValue = rand() % 10000;
		return currBtreeIdsetsValue;
	}

	Item GenerateDefaultNsItem(int idValue, size_t packagesCount) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = idValue;
		item[kFieldNameYear] = rand() % 50 + 2000;
		item[kFieldNameGenre] = rand() % 50;
		item[kFieldNameName] = RandString().c_str();
		item[kFieldNameAge] = rand() % 50;
		item[kFieldNameDescription] = RandString().c_str();

		auto packagesVec(RandIntVector(packagesCount, 10000, 50));
		item[kFieldNamePackages] = packagesVec;

		item[kFieldNameRate] = static_cast<double>(rand() % 100) / 10;

		auto pricesIds(RandIntVector(10, 7000, 50));
		item[kFieldNamePriceId] = pricesIds;

		int stTime = rand() % 50000;
		item[kFieldNameLocation] = RandString().c_str();
		item[kFieldNameStartTime] = stTime;
		item[kFieldNameEndTime] = stTime + (rand() % 5) * 1000;
		item[kFieldNameActor] = RandString().c_str();
		item[kFieldNameNumeric] = to_string(rand() % 1000);
		item[kFieldNameBtreeIdsets] = GetcurrBtreeIdsetsValue(idValue);

		return item;
	}

	void CheckDistinctQueries() {
		static const vector<string> distincts = {"", kFieldNameYear, kFieldNameRate};

		for (const string& distinct : distincts) {
			const int randomAge = rand() % 50;
			const int randomGenre = rand() % 50;

			ExecuteAndVerifyWithSql(
				default_namespace,
				Query(default_namespace).Where(kFieldNameGenre, CondEq, randomGenre).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(
				default_namespace,
				Query(default_namespace).Where(kFieldNameName, CondEq, RandString()).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(default_namespace, Query(default_namespace)
														   .Where(kFieldNameRate, CondEq, static_cast<double>(rand() % 100) / 10)
														   .Distinct(distinct.c_str())
														   .Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(default_namespace, Query(default_namespace)
														   .Where(kFieldNameGenre, CondGt, randomGenre)
														   .Distinct(distinct.c_str())
														   .Sort(kFieldNameYear, true)
														   .Debug(LogTrace));

			ExecuteAndVerifyWithSql(
				default_namespace,
				Query(default_namespace).Where(kFieldNameName, CondGt, RandString()).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(default_namespace, Query(default_namespace)
														   .Where(kFieldNameRate, CondGt, static_cast<double>(rand() % 100) / 10)
														   .Distinct(distinct.c_str())
														   .Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(
				default_namespace,
				Query(default_namespace).Where(kFieldNameGenre, CondLt, randomGenre).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(default_namespace, Query(default_namespace)
														   .Where(kFieldNameAge, CondEq, randomAge)
														   .Where(kFieldNameGenre, CondEq, randomGenre)
														   .Distinct(distinct.c_str())
														   .Sort(kFieldNameYear, true));
		}
	}

	void CheckStandartQueries() {
		static const vector<string> sortIdxs = {"",
												kFieldNameName,
												kFieldNameYear,
												kFieldNameRate,
												kFieldNameBtreeIdsets,
												string{"-2.5 * "} + kFieldNameRate + " / (" + kFieldNameYear + " + " + kFieldNameId + ')'};
		static const vector<string> distincts = {"", kFieldNameYear, kFieldNameRate};
		static const vector<bool> sortOrders = {true, false};

		static const string compositeIndexName(kFieldNameAge + compositePlus + kFieldNameGenre);

		for (const bool sortOrder : sortOrders) {
			for (const string& sortIdx : sortIdxs) {
				for (const string& distinct : distincts) {
					const int randomAge = rand() % 50;
					const int randomGenre = rand() % 50;
					const int randomGenreUpper = rand() % 100;
					const int randomGenreLower = rand() % 100;

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder).Limit(1));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondEq, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondEq, RandString())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameRate, CondEq, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondGt, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondGt, RandString())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameRate, CondGt, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondLt, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondLt, RandString())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameRate, CondLt, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameBtreeIdsets, CondLt, static_cast<int>(rand() % 10000))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameBtreeIdsets, CondGt, static_cast<int>(rand() % 10000))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameBtreeIdsets, CondEq, static_cast<int>(rand() % 10000))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondRange, {randomGenreLower, randomGenreUpper})
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondLike, RandLikePattern())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace)
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Distinct(distinct.c_str())
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNamePackages, CondSet, RandIntVector(10, 10000, 50))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					// check substituteCompositIndexes
					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameAge, CondEq, randomAge)
															.Where(kFieldNameGenre, CondEq, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
															.Where(kFieldNameGenre, CondEq, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
															.Where(kFieldNameGenre, CondSet, RandIntVector(10, 0, 50))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));
					// end of check substituteCompositIndexes

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNamePackages, CondEmpty, 0)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(kFieldNameYear, true)
															.Sort(kFieldNameName, false)
															.Sort(kFieldNameLocation, true));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(kFieldNameGenre, true)
															.Sort(kFieldNameActor, false)
															.Sort(kFieldNameRate, true)
															.Sort(kFieldNameLocation, false));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondLike, RandLikePattern())
															.Distinct(distinct.c_str())
															.Sort(kFieldNameGenre, true)
															.Sort(kFieldNameActor, false)
															.Sort(kFieldNameRate, true)
															.Sort(kFieldNameLocation, false));

					ExecuteAndVerify(default_namespace, Query(default_namespace).Sort(kFieldNameGenre, true, {10, 20, 30}));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(kFieldNamePackages, CondAny, 0).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(kFieldNameIsDeleted, CondEq, 1).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Where(kFieldNameAge, CondEq, 3)
															.Where(kFieldNameYear, CondGe, 2010)
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameYear, CondGt, 2002)
															.Where(kFieldNameGenre, CondEq, 4)
															.Where(kFieldNameAge, CondEq, 3)
															.Where(kFieldNameIsDeleted, CondEq, 3)
															.Or()
															.Where(kFieldNameYear, CondGt, 2001)
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameAge, CondSet, {1, 2, 3, 4})
															.Where(kFieldNameId, CondEq, rand() % 5000)
															.Where(kFieldNameTemp, CondEq, "")
															.Where(kFieldNameIsDeleted, CondEq, 1)
															.Or()
															.Where(kFieldNameYear, CondGt, 2001)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondSet, {5, 1, 7})
															.Where(kFieldNameYear, CondLt, 2010)
															.Where(kFieldNameGenre, CondEq, 3)
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Or()
															.Where(kFieldNamePackages, CondEmpty, 0)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondSet, {5, 1, 7})
															.Where(kFieldNameYear, CondLt, 2010)
															.Or()
															.Where(kFieldNamePackages, CondAny, 0)
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Or()
															.Where(kFieldNameGenre, CondEq, 6)
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Or()
															.Where(kFieldNameGenre, CondEq, 6)
															.Not()
															.Where(kFieldNameName, CondLike, RandLikePattern())
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameActor, CondEq, RandString()));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Not()
															.Where(kFieldNameGenre, CondEq, 5)
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Not()
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Not()
															.Where(kFieldNameYear, CondEq, 10));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(kFieldNameNumeric, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameNumeric, CondGt, std::to_string(5)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(kFieldNameNumeric, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameNumeric, CondLt, std::to_string(600)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Or()
															.OpenBracket()
															.Where(kFieldNameGenre, CondLt, 6)
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.CloseBracket()
															.Not()
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.OpenBracket()
															.Where(kFieldNameNumeric, CondLt, std::to_string(600))
															.Or()
															.OpenBracket()
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Where(kFieldNameName, CondLike, RandLikePattern())
															.CloseBracket()
															.Or()
															.Where(kFieldNameYear, CondEq, 10)
															.CloseBracket());

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Not()
															.OpenBracket()
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Or()
															.Where(kFieldNameName, CondLike, RandLikePattern())
															.CloseBracket()
															.Or()
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.OpenBracket()
															.Where(kFieldNameNumeric, CondLt, std::to_string(600))
															.Not()
															.OpenBracket()
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Where(kFieldNameGenre, CondLt, 6)
															.CloseBracket()
															.Or()
															.Where(kFieldNameYear, CondEq, 10)
															.CloseBracket());

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameNumeric, CondRange,
																   {std::to_string(rand() % 100), std::to_string(rand() % 100 + 500)}));

					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS"));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 1989));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, "MMM"));

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace)
										 .ReqTotal()
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .WhereComposite(compositeIndexName.c_str(), CondLe, {{Variant(27), Variant(10000)}}));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.ReqTotal()
															.Distinct(distinct)
															.Sort(sortIdx, sortOrder)
															.WhereComposite(compositeIndexName.c_str(), CondEq,
																			{{Variant(rand() % 10), Variant(rand() % 50)}}));
				}
			}
		}
	}

	template <typename FacetMap>
	static void frameFacet(FacetMap& facet, size_t offset, size_t limit) {
		if (offset >= facet.size()) {
			facet.clear();
		} else {
			auto end = facet.begin();
			std::advance(end, offset);
			facet.erase(facet.begin(), end);
		}
		if (limit < facet.size()) {
			auto begin = facet.begin();
			std::advance(begin, limit);
			facet.erase(begin, facet.end());
		}
	}

	static void checkFacetValues(const reindexer::h_vector<std::string, 1>& result, const std::string& expected, const std::string& name) {
		ASSERT_EQ(result.size(), 1) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(result[0], expected) << (name + " aggregation Facet result is incorrect!");
	}

	static void checkFacetValues(const reindexer::h_vector<std::string, 1>& result, int expected, const std::string& name) {
		ASSERT_EQ(result.size(), 1) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(std::stoi(result[0]), expected) << (name + " aggregation Facet result is incorrect!");
	}

	template <typename T>
	static void checkFacetValues(const reindexer::h_vector<std::string, 1>& result, const T& expected, const std::string& name) {
		ASSERT_EQ(result.size(), 2) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(result[0], expected.name) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(std::stoi(result[1]), expected.year) << (name + " aggregation Facet result is incorrect!");
	}

	template <typename ExpectedFacet>
	static void checkFacet(const reindexer::h_vector<reindexer::FacetResult, 1>& result, const ExpectedFacet& expected,
						   const std::string& name) {
		ASSERT_EQ(result.size(), expected.size()) << (name + " aggregation Facet result is incorrect!");
		auto resultIt = result.begin();
		auto expectedIt = expected.cbegin();
		for (; resultIt != result.end() && expectedIt != expected.cend(); ++resultIt, ++expectedIt) {
			checkFacetValues(resultIt->values, expectedIt->first, name);
			EXPECT_EQ(resultIt->count, expectedIt->second) << (name + " aggregation Facet result is incorrect!");
		}
	}

	void CheckAggregationQueries() {
		constexpr size_t facetLimit = 10;
		constexpr size_t facetOffset = 10;

		const Query wrongQuery1 = Query(default_namespace).Aggregate(AggAvg, {});
		reindexer::QueryResults wrongQr1;
		Error err = rt.reindexer->Select(wrongQuery1, wrongQr1);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Empty set of fields for aggregation avg");

		const Query wrongQuery2 = Query(default_namespace).Aggregate(AggAvg, {kFieldNameYear, kFieldNameName});
		reindexer::QueryResults wrongQr2;
		err = rt.reindexer->Select(wrongQuery2, wrongQr2);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "For aggregation avg available exactly one field");

		const Query wrongQuery3 = Query(default_namespace).Aggregate(AggAvg, {kFieldNameYear}, {{kFieldNameYear, true}});
		reindexer::QueryResults wrongQr3;
		err = rt.reindexer->Select(wrongQuery3, wrongQr3);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Sort is not available for aggregation avg");

		const Query wrongQuery4 = Query(default_namespace).Aggregate(AggAvg, {kFieldNameYear}, {}, 10);
		reindexer::QueryResults wrongQr4;
		err = rt.reindexer->Select(wrongQuery4, wrongQr4);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Limit or offset are not available for aggregation avg");

		const Query wrongQuery5 = Query(default_namespace).Aggregate(AggFacet, {kFieldNameYear}, {{kFieldNameName, true}});
		reindexer::QueryResults wrongQr5;
		err = rt.reindexer->Select(wrongQuery5, wrongQr5);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "The aggregation facet cannot provide sort by 'name'");

		const Query wrongQuery6 = Query(default_namespace).Aggregate(AggFacet, {kFieldNameCountries, kFieldNameYear});
		reindexer::QueryResults wrongQr6;
		err = rt.reindexer->Select(wrongQuery6, wrongQr6);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Multifield facet cannot contain an array field");

		Query testQuery = Query(default_namespace)
							  .Aggregate(AggAvg, {kFieldNameYear})
							  .Aggregate(AggSum, {kFieldNameYear})
							  .Aggregate(AggMin, {kFieldNamePackages})
							  .Aggregate(AggFacet, {kFieldNameName}, {{"Count", false}}, facetLimit, facetOffset)
							  .Aggregate(AggFacet, {kFieldNamePackages}, {}, facetLimit, facetOffset)
							  .Aggregate(AggFacet, {kFieldNameName, kFieldNameYear}, {{kFieldNameYear, true}, {kFieldNameName, false}},
										 facetLimit, facetOffset);
		Query checkQuery = Query(default_namespace);

		reindexer::QueryResults testQr;
		err = rt.reindexer->Select(testQuery, testQr);
		EXPECT_TRUE(err.ok()) << err.what();

		reindexer::QueryResults checkQr;
		err = rt.reindexer->Select(checkQuery, checkQr);
		EXPECT_TRUE(err.ok()) << err.what();

		double yearSum = 0.0;
		int packagesMin = std::numeric_limits<int>::max();
		struct MultifieldFacetItem {
			std::string name;
			int year;
			bool operator<(const MultifieldFacetItem& other) const {
				if (year == other.year) return name < other.name;
				return year > other.year;
			}
		};
		std::map<MultifieldFacetItem, int> multifieldFacet;
		std::unordered_map<std::string, int> singlefieldFacetMap;
		std::map<int, int> arrayFacet;
		for (auto it : checkQr) {
			Item item(it.GetItem());
			yearSum += item[kFieldNameYear].Get<int>();
			++multifieldFacet[MultifieldFacetItem{string(item[kFieldNameName].Get<reindexer::string_view>()),
												  item[kFieldNameYear].Get<int>()}];
			++singlefieldFacetMap[string(item[kFieldNameName].Get<reindexer::string_view>())];
			for (const Variant& pack : static_cast<reindexer::VariantArray>(item[kFieldNamePackages])) {
				const int value = pack.As<int>();
				packagesMin = std::min(value, packagesMin);
				++arrayFacet[value];
			}
		}
		std::vector<std::pair<std::string, int>> singlefieldFacet(singlefieldFacetMap.begin(), singlefieldFacetMap.end());
		std::sort(singlefieldFacet.begin(), singlefieldFacet.end(),
				  [](const std::pair<std::string, int>& lhs, const std::pair<std::string, int>& rhs) {
					  return lhs.second == rhs.second ? lhs.first < rhs.first : lhs.second < rhs.second;
				  });
		frameFacet(multifieldFacet, facetOffset, facetLimit);
		frameFacet(singlefieldFacet, facetOffset, facetLimit);
		frameFacet(arrayFacet, facetOffset, facetLimit);

		EXPECT_DOUBLE_EQ(testQr.aggregationResults[0].value, yearSum / checkQr.Count()) << "Aggregation Avg result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.aggregationResults[1].value, yearSum) << "Aggregation Sum result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.aggregationResults[2].value, packagesMin) << "Aggregation Min result is incorrect!";
		checkFacet(testQr.aggregationResults[3].facets, singlefieldFacet, "Singlefield");
		checkFacet(testQr.aggregationResults[4].facets, arrayFacet, "Array");
		checkFacet(testQr.aggregationResults[5].facets, multifieldFacet, "Multifield");
	}

	void CompareQueryResults(const QueryResults& lhs, const QueryResults& rhs) {
		ASSERT_EQ(lhs.Count(), rhs.Count());
		for (size_t i = 0; i < rhs.Count(); ++i) {
			Item ritem1(rhs[i].GetItem());
			Item ritem2(lhs[i].GetItem());
			EXPECT_EQ(ritem1.NumFields(), ritem2.NumFields());
			if (ritem1.NumFields() == ritem2.NumFields()) {
				for (int idx = 1; idx < ritem1.NumFields(); ++idx) {
					const VariantArray& v1 = ritem1[idx];
					const VariantArray& v2 = ritem2[idx];

					ASSERT_EQ(v1.size(), v2.size());
					for (size_t j = 0; j < v1.size(); ++j) {
						EXPECT_EQ(v1[j].Compare(v2[j]), 0);
					}
				}
			}
		}

		ASSERT_EQ(lhs.aggregationResults.size(), rhs.aggregationResults.size());
		for (size_t i = 0; i < rhs.aggregationResults.size(); ++i) {
			const auto& aggRes1 = rhs.aggregationResults[i];
			const auto& aggRes2 = lhs.aggregationResults[i];
			EXPECT_EQ(aggRes1.type, aggRes2.type);
			EXPECT_DOUBLE_EQ(aggRes1.value, aggRes2.value);
			ASSERT_EQ(aggRes1.fields.size(), aggRes2.fields.size());
			for (size_t j = 0; j < aggRes1.fields.size(); ++j) {
				EXPECT_EQ(aggRes1.fields[j], aggRes2.fields[j]);
			}
			ASSERT_EQ(aggRes1.facets.size(), aggRes2.facets.size());
			for (size_t j = 0; j < aggRes1.facets.size(); ++j) {
				EXPECT_EQ(aggRes1.facets[j].count, aggRes2.facets[j].count);
				ASSERT_EQ(aggRes1.facets[j].values.size(), aggRes2.facets[j].values.size());
				for (size_t k = 0; k < aggRes1.facets[j].values.size(); ++k) {
					EXPECT_EQ(aggRes1.facets[j].values[k], aggRes2.facets[j].values[k]) << aggRes1.facets[j].values[0];
				}
			}
		}
	}

	void CheckSqlQueries() {
		string sqlQuery = "SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery1 = Query(default_namespace, 0, 10000000).Where(kFieldNameYear, CondGt, 2016).Sort(kFieldNameYear, true);

		QueryResults sqlQr;
		Error err = rt.reindexer->Select(sqlQuery, sqlQr);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr;
		err = rt.reindexer->Select(checkQuery1, checkQr);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr, checkQr);
		Verify(default_namespace, checkQr, checkQuery1);

		sqlQuery = "SELECT ID, Year, Genre FROM test_namespace WHERE genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery2 =
			Query(default_namespace, 0, 10000000).Where(kFieldNameGenre, CondSet, {1, 2, 3}).Sort(kFieldNameYear, true);

		QueryResults sqlQr2;
		err = rt.reindexer->Select(sqlQuery, sqlQr2);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr2;
		err = rt.reindexer->Select(checkQuery2, checkQr2);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr2, checkQr2);
		Verify(default_namespace, checkQr2, checkQuery2);

		const string likePattern = RandLikePattern();
		sqlQuery = "SELECT ID, Year, Genre FROM test_namespace WHERE name LIKE '" + likePattern + "' ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery3 =
			Query(default_namespace, 0, 10000000).Where(kFieldNameName, CondLike, likePattern).Sort(kFieldNameYear, true);

		QueryResults sqlQr3;
		err = rt.reindexer->Select(sqlQuery, sqlQr3);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr3;
		err = rt.reindexer->Select(checkQuery3, checkQr3);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr3, checkQr3);
		Verify(default_namespace, checkQr3, checkQuery3);

		sqlQuery = "SELECT ID, FACET(ID, Year ORDER BY ID DESC ORDER BY Year ASC LIMIT 20 OFFSET 1) FROM test_namespace LIMIT 10000000";
		const Query checkQuery4 =
			Query(default_namespace, 0, 10000000)
				.Aggregate(AggFacet, {kFieldNameId, kFieldNameYear}, {{kFieldNameId, true}, {kFieldNameYear, false}}, 20, 1);

		QueryResults sqlQr4;
		err = rt.reindexer->Select(sqlQuery, sqlQr4);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr4;
		err = rt.reindexer->Select(checkQuery4, checkQr4);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr4, checkQr4);
		Verify(default_namespace, checkQr4, checkQuery4);

		sqlQuery = "SELECT ID FROM test_namespace WHERE name LIKE '" + likePattern +
				   "' AND (genre IN ('1', '2', '3') AND year > '2016' ) OR age IN ('1', '2', '3', '4') LIMIT 10000000";
		const Query checkQuery5 = Query(default_namespace, 0, 10000000)
									  .Where(kFieldNameName, CondLike, likePattern)
									  .OpenBracket()
									  .Where(kFieldNameGenre, CondSet, {1, 2, 3})
									  .Where(kFieldNameYear, CondGt, 2016)
									  .CloseBracket()
									  .Or()
									  .Where(kFieldNameAge, CondSet, {1, 2, 3, 4});

		QueryResults sqlQr5;
		err = rt.reindexer->Select(sqlQuery, sqlQr5);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr5;
		err = rt.reindexer->Select(checkQuery5, checkQr5);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr5, checkQr5);
		Verify(default_namespace, checkQr5, checkQuery5);

		sqlQuery = string("SELECT ID FROM test_namespace ORDER BY '") + kFieldNameYear + " + " + kFieldNameId + " * 5' DESC LIMIT 10000000";
		const Query checkQuery6 =
			Query(default_namespace, 0, 10000000).Sort(kFieldNameYear + std::string(" + ") + kFieldNameId + " * 5", true);

		QueryResults sqlQr6;
		err = rt.reindexer->Select(sqlQuery, sqlQr6);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr6;
		err = rt.reindexer->Select(checkQuery6, checkQr6);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr6, checkQr6);
		Verify(default_namespace, checkQr6, checkQuery6);

		sqlQuery = string("SELECT ID FROM test_namespace ORDER BY '") + kFieldNameYear + " + " + kFieldNameId +
				   " * 5' DESC ORDER BY '2 * " + kFieldNameGenre + " / (1 + " + kFieldNameIsDeleted + ")' ASC LIMIT 10000000";
		const Query checkQuery7 = Query(default_namespace, 0, 10000000)
									  .Sort(kFieldNameYear + string(" + ") + kFieldNameId + " * 5", true)
									  .Sort(string("2 * ") + kFieldNameGenre + " / (1 + " + kFieldNameIsDeleted + ')', false);

		QueryResults sqlQr7;
		err = rt.reindexer->Select(sqlQuery, sqlQr7);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr7;
		err = rt.reindexer->Select(checkQuery7, checkQr7);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQr7, checkQr7);
		Verify(default_namespace, checkQr7, checkQuery7);
	}

	void CheckCompositeIndexesQueries() {
		int priceValue = 77777;
		int pagesValue = 88888;
		const char* titleValue = "test book1 title";
		const char* nameValue = "test book1 name";

		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondEq, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondLt, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondLe, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondGt, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondGe, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldPricePages.c_str(), CondRange,
																 {{Variant(1), Variant(1)}, {Variant(priceValue), Variant(pagesValue)}}));

		vector<VariantArray> intKeys;
		for (int i = 0; i < 10; ++i) {
			intKeys.emplace_back(VariantArray{Variant(i), Variant(i * 5)});
		}
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs).WhereComposite(kCompositeFieldPricePages.c_str(), CondSet, intKeys));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
																 {{Variant(string(titleValue)), Variant(string(nameValue))}}));
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondGe,
																 {{Variant(string(titleValue)), Variant(string(nameValue))}}));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondLt,
																 {{Variant(string(titleValue)), Variant(string(nameValue))}}));
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondLe,
																 {{Variant(string(titleValue)), Variant(string(nameValue))}}));
		vector<VariantArray> stringKeys;
		for (size_t i = 0; i < 1010; ++i) {
			stringKeys.emplace_back(VariantArray{Variant(RandString()), Variant(RandString())});
		}
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs).WhereComposite(kCompositeFieldTitleName.c_str(), CondSet, stringKeys));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .Where(kFieldNameName, CondEq, nameValue)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
																 {{Variant(string(titleValue)), Variant(string(nameValue))}}));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs));
	}

	void CheckComparatorsQueries() {
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnInt64", CondLe, {Variant(static_cast<int64_t>(10000))}));

		vector<double> doubleSet;
		for (size_t i = 0; i < 1010; i++) {
			doubleSet.emplace_back(static_cast<double>(rand()) / RAND_MAX);
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnDouble", CondSet, doubleSet));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondGe, string("test_string1")));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondLe, string("test_string2")));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondEq, string("test_string3")));

		vector<string> stringSet;
		for (size_t i = 0; i < 1010; i++) {
			stringSet.emplace_back(RandString());
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 100; i++) {
			stringSet.emplace_back(std::to_string(i + 20000));
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnStringNumeric", CondSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 100; i++) {
			stringSet.emplace_back(std::to_string(i + 1));
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnStringNumeric", CondSet, stringSet));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnStringNumeric", CondEq, string("777")));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnFullText", CondEq, RandString()));
	}

	static void PrintFailedQueryEntries(const reindexer::QueryEntries& failedEntries) {
		TestCout() << "Failed entries: ";
		PrintQueryEntries(failedEntries.cbegin(), failedEntries.cend());
		TestCout() << std::endl << std::endl;
	}

	static void PrintQueryEntries(reindexer::QueryEntries::const_iterator it, reindexer::QueryEntries::const_iterator to) {
		TestCout() << "(";
		for (; it != to; ++it) {
			TestCout() << (it->Op == OpAnd ? "AND" : (it->Op == OpOr ? "OR" : "NOT"));
			if (it->IsLeaf()) {
				TestCout() << it->Value().Dump();
			} else {
				PrintQueryEntries(it.cbegin(), it.cend());
			}
		}
		TestCout() << ")";
	}

	static void boldOn() { TestCout() << "\e[1m"; }
	static void boldOff() { TestCout() << "\e[0m"; }

	void PrintFailedSortOrder(const Query& query, const QueryResults& qr, int itemIndex, int itemsToShow = 10) {
		if (qr.Count() == 0) return;

		TestCout() << "Sort order or last items:" << std::endl;
		Item rdummy(qr[0].GetItem());
		boldOn();
		for (size_t idx = 0; idx < query.sortingEntries_.size(); idx++) {
			TestCout() << rdummy[query.sortingEntries_[idx].expression].Name() << " ";
		}
		boldOff();
		TestCout() << std::endl << std::endl;

		int firstItem = itemIndex - itemsToShow;
		if (firstItem < 0) firstItem = 0;
		for (int i = firstItem; i <= itemIndex; ++i) {
			Item item(qr[i].GetItem());
			if (i == itemIndex) boldOn();
			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				TestCout() << item[query.sortingEntries_[j].expression].As<string>() << " ";
			}
			if (i == itemIndex) boldOff();
			TestCout() << std::endl;
		}

		firstItem = itemIndex + 1;
		int lastItem = firstItem + itemsToShow;
		const int count = static_cast<int>(qr.Count());
		if (firstItem >= count) firstItem = count - 1;
		if (lastItem > count) lastItem = count;
		for (int i = firstItem; i < lastItem; ++i) {
			Item item(qr[i].GetItem());
			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				TestCout() << item[query.sortingEntries_[j].expression].As<string>() << " ";
			}
			TestCout() << std::endl;
		}

		TestCout() << std::endl << std::endl;
	}

	using NamespaceName = string;
	using InsertedItemsByPk = std::map<string, reindexer::Item>;
	std::unordered_map<NamespaceName, InsertedItemsByPk> insertedItems;
	std::unordered_map<string, IndexOpts> indexesOptions;

	const char* kFieldNameId = "id";
	const char* kFieldNameGenre = "genre";
	const char* kFieldNameYear = "year";
	const char* kFieldNamePackages = "packages";
	const char* kFieldNameName = "name";
	const char* kFieldNameCountries = "countries";
	const char* kFieldNameAge = "age";
	const char* kFieldNameDescription = "description";
	const char* kFieldNameRate = "rate";
	const char* kFieldNameIsDeleted = "is_deleted";
	const char* kFieldNameActor = "actor";
	const char* kFieldNamePriceId = "price_id";
	const char* kFieldNameLocation = "location";
	const char* kFieldNameEndTime = "end_time";
	const char* kFieldNameStartTime = "start_time";
	const char* kFieldNamePhone = "phone";
	const char* kFieldNameTemp = "tmp";
	const char* kFieldNameNumeric = "numeric";
	const char* kFieldNameBookid = "bookid";
	const char* kFieldNameBookid2 = "bookid2";
	const char* kFieldNameTitle = "title";
	const char* kFieldNamePages = "pages";
	const char* kFieldNamePrice = "price";
	const char* kFieldNameBtreeIdsets = "btree_idsets";

	const char* kFieldNameColumnInt = "columnInt";
	const char* kFieldNameColumnInt64 = "columnInt64";
	const char* kFieldNameColumnDouble = "columnDouble";
	const char* kFieldNameColumnString = "columnString";
	const char* kFieldNameColumnFullText = "columnFullText";
	const char* kFieldNameColumnStringNumeric = "columnStringNumeric";

	const char* kFieldNameColumnHash = "columnHash";
	const char* kFieldNameColumnTree = "columnTree";

	const string compositePlus = "+";
	const string testSimpleNs = "test_simple_namespace";
	const string compositeIndexesNs = "composite_indexes_namespace";
	const string comparatorsNs = "comparators_namespace";
	const string forcedSortOffsetNs = "forced_sort_offset_namespace";

	const string kCompositeFieldPricePages = kFieldNamePrice + compositePlus + kFieldNamePages;
	const string kCompositeFieldTitleName = kFieldNameTitle + compositePlus + kFieldNameName;

	vector<string> defaultNsPks;
	vector<string> simpleTestNsPks;
	vector<string> compositeIndexesNsPks;
	vector<string> comparatorsNsPks;
	vector<string> forcedSortOffsetNsPks;
	std::mutex m_;

	int currBtreeIdsetsValue = rand() % 10000;
	static constexpr size_t forcedSortOffsetNsSize = 1000;
	static constexpr int forcedSortOffsetMaxValue = 1000;
	vector<pair<int, int>> forcedSortOffsetValues;
};
