#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "estl/h_vector.h"
#include "estl/span.h"
#include "tools/errors.h"
#include "tools/stringstools.h"
#include "vendor/hopscotch/hopscotch_map.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class TagsMatcher;
class PayloadType;
class WrSerializer;
class ProtobufSchemaBuilder;

string_view kvTypeToJsonSchemaType(KeyValueType type);

class FieldProps {
public:
	FieldProps() = default;
	FieldProps(KeyValueType _type, bool _isArray = false, bool _isRequired = false, bool _allowAdditionalProps = false,
			   const std::string& _xGoType = {})
		: type(kvTypeToJsonSchemaType(_type)),
		  xGoType(_xGoType),
		  isArray(_isArray),
		  isRequired(_isRequired),
		  allowAdditionalProps(_allowAdditionalProps) {}
	FieldProps(std::string _type, bool _isArray = false, bool _isRequired = false, bool _allowAdditionalProps = false,
			   const std::string& _xGoType = {})
		: type(std::move(_type)),
		  xGoType(_xGoType),
		  isArray(_isArray),
		  isRequired(_isRequired),
		  allowAdditionalProps(_allowAdditionalProps) {}
	FieldProps(FieldProps&&) = default;
	FieldProps& operator=(FieldProps&&) = default;

	bool operator==(const FieldProps& rh) const {
		return type == rh.type && isArray == rh.isArray && isRequired == rh.isRequired && allowAdditionalProps == rh.allowAdditionalProps;
	}

	std::string type;
	std::string xGoType;
	bool isArray = false;
	bool isRequired = false;
	bool allowAdditionalProps = false;
};

class Schema;

struct SchemaFieldType {
	KeyValueType type_;
	bool isArray_;
};

class SchemaFieldsTypes {
public:
	void AddObject(string_view objectType);
	void AddField(KeyValueType type, bool isArray);
	KeyValueType GetField(const TagsPath& fieldPath, bool& isArray) const;
	bool ContainsObjectType(string objectType) const;

private:
	friend class ProtobufSchemaBuilder;

	TagsPath tagsPath_;
	std::unordered_map<TagsPath, SchemaFieldType> types_;
	std::unordered_set<string> objectTypes_;
};

class PrefixTree {
public:
	using PathT = h_vector<std::string, 10>;

	PrefixTree();

	void SetXGoType(string_view type);

	Error AddPath(FieldProps props, const PathT& splittedPath) noexcept;
	std::vector<std::string> GetSuggestions(string_view path) const;
	std::vector<std::string> GetPaths() const;
	bool HasPath(string_view path, bool allowAdditionalFields) const noexcept;
	Error BuildProtobufSchema(WrSerializer& schema, TagsMatcher& tm, PayloadType& pt) noexcept;

	struct PrefixTreeNode;
	using map = tsl::hopscotch_map<std::string, std::unique_ptr<PrefixTreeNode>, hash_str, equal_str>;
	struct PrefixTreeNode {
		void GetPaths(std::string&& basePath, std::vector<std::string>& pathsList) const;

		FieldProps props_;
		map children_;
	};

private:
	friend Schema;
	static std::string pathToStr(const PathT&);
	PrefixTreeNode* findNode(string_view path, bool* maybeAdditionalField = nullptr) const noexcept;
	Error buildProtobufSchema(ProtobufSchemaBuilder& builder, const PrefixTreeNode& node, const std::string& basePath,
							  TagsMatcher& tm) noexcept;

	PrefixTreeNode root_;
	mutable SchemaFieldsTypes fieldsTypes_;
};

class Schema {
public:
	Schema() = default;
	explicit Schema(string_view json);

	std::vector<string> GetSuggestions(string_view path) const { return paths_.GetSuggestions(path); }
	std::vector<std::string> GetPaths() const noexcept { return paths_.GetPaths(); }
	KeyValueType GetFieldType(const TagsPath& fieldPath, bool& isArray) const;

	bool HasPath(string_view path, bool allowAdditionalFields = false) const noexcept {
		return paths_.HasPath(path, allowAdditionalFields);
	}

	Error FromJSON(string_view json);
	void GetJSON(WrSerializer&) const;
	Error BuildProtobufSchema(TagsMatcher& tm, PayloadType& pt);
	Error GetProtobufSchema(WrSerializer& schema) const;
	int GetProtobufNsNumber() const { return protobufNsNumber_; }
	const PrefixTree::PrefixTreeNode* GetRoot() const { return &paths_.root_; }

private:
	void parseJsonNode(const gason::JsonNode& node, PrefixTree::PathT& splittedPath, bool isRequired);

	PrefixTree paths_;
	std::string originalJson_;
	std::string protobufSchema_;
	Error protobufSchemaStatus_;
	int protobufNsNumber_;
};

}  // namespace reindexer
