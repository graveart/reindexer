#include "protobufbuilder.h"

namespace reindexer {

ProtobufBuilder::ProtobufBuilder(WrSerializer* wrser, ObjType type, const TagsMatcher* tm, int fieldIdx)
	: type_(type), ser_(wrser), tm_(tm), sizeHelper_(), itemsFieldIndex_(fieldIdx) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObject:
			putFieldHeader(fieldIdx, PBUF_TYPE_LENGTHENCODED);
			sizeHelper_ = ser_->StartVString();
			break;
		case ObjType::TypeObjectArray:
			itemsFieldIndex_ = fieldIdx;
			break;
		default:
			break;
	}
}

void ProtobufBuilder::End() {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObject: {
			sizeHelper_.End();
			break;
		}
		case ObjType::TypeObjectArray:
			itemsFieldIndex_ = -1;
			break;
		default:
			break;
	}
	type_ = ObjType::TypePlain;
}

void ProtobufBuilder::packItem(int fieldIdx, int tagType, Serializer& rdser, ProtobufBuilder& array) {
	switch (tagType) {
		case TAG_DOUBLE:
			array.put(fieldIdx, rdser.GetDouble());
			break;
		case TAG_VARINT:
			array.put(fieldIdx, rdser.GetVarint());
			break;
		case TAG_BOOL:
			array.put(fieldIdx, rdser.GetBool());
			break;
		case TAG_STRING:
			array.put(fieldIdx, string(rdser.GetVString()));
			break;
		case TAG_NULL:
			array.Null(fieldIdx);
			break;
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", ctag(tagType).TypeName());
	}
}

int ProtobufBuilder::getFieldTag(int fieldIdx) const {
	if (type_ == ObjType::TypeObjectArray && itemsFieldIndex_ != -1) {
		return itemsFieldIndex_;
	}
	return fieldIdx;
}

void ProtobufBuilder::putFieldHeader(int fieldIdx, ProtobufTypes type) {
	if (type_ == ObjType::TypeObjectArray && itemsFieldIndex_ != -1) {
		fieldIdx = itemsFieldIndex_;
	}
	ser_->PutVarUint((getFieldTag(fieldIdx) << kNameBit) | type);
}

void ProtobufBuilder::put(int fieldIdx, bool val) { return put(fieldIdx, int(val)); }

void ProtobufBuilder::put(int fieldIdx, int val) {
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_VARINT);
	}
	ser_->PutVarUint(val);
}

void ProtobufBuilder::put(int fieldIdx, int64_t val) {
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_VARINT);
	}
	ser_->PutVarUint(val);
}

void ProtobufBuilder::put(int fieldIdx, double val) {
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_FLOAT64);
	}
	ser_->PutDouble(val);
}

void ProtobufBuilder::put(int fieldIdx, string_view val) {
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutVString(val);
}

void ProtobufBuilder::put(int fieldIdx, const Variant& kv) {
	switch (kv.Type()) {
		case KeyValueInt64:
			put(fieldIdx, int64_t(kv));
			break;
		case KeyValueInt:
			put(fieldIdx, int(kv));
			break;
		case KeyValueDouble:
			put(fieldIdx, double(kv));
			break;
		case KeyValueString:
			put(fieldIdx, string_view(kv));
			break;
		case KeyValueBool:
			put(fieldIdx, bool(kv));
			break;
		case KeyValueTuple: {
			auto arrNode = ArrayPacked(fieldIdx);
			for (auto& val : kv.getCompositeValues()) {
				arrNode.Put(fieldIdx, val);
			}
			break;
		}
		case KeyValueNull:
			break;
		default:
			break;
	}
}

ProtobufBuilder ProtobufBuilder::Object(int fieldIdx, int) {
	// Main object in Protobuf is never of Object type,
	// only nested object fields are.
	if (type_ == ObjType::TypePlain && fieldIdx == 0) {
		return ProtobufBuilder(std::move(*this));
	}
	return ProtobufBuilder(ser_, ObjType::TypeObject, tm_, getFieldTag(fieldIdx));
}

}  // namespace reindexer
