#include "indexopts.h"

CollateOpts::CollateOpts(uint8_t mode) : mode(mode) {}

CollateOpts::CollateOpts(const std::string& sortOrderUTF8) : mode(CollateCustom), sortOrderTable(sortOrderUTF8) {}

IndexOpts::IndexOpts(uint8_t flags, CollateMode mode) : options(flags), collateOpts_(mode) {}

IndexOpts::IndexOpts(const std::string& sortOrderUTF8, uint8_t flags) : options(flags), collateOpts_(sortOrderUTF8) {}

bool IndexOpts::IsEqual(const IndexOpts& other, bool skipConfig) const {
	return options == other.options && (skipConfig || config == other.config) && collateOpts_.mode == other.collateOpts_.mode &&
		   collateOpts_.sortOrderTable.GetSortOrderCharacters() == other.collateOpts_.sortOrderTable.GetSortOrderCharacters();
}

bool IndexOpts::IsPK() const noexcept { return options & kIndexOptPK; }
bool IndexOpts::IsArray() const noexcept { return options & kIndexOptArray; }
bool IndexOpts::IsDense() const noexcept { return options & kIndexOptDense; }
bool IndexOpts::IsSparse() const noexcept { return options & kIndexOptSparse; }
bool IndexOpts::IsRTreeLinear() const noexcept { return options & kIndexOptRTreeLinear; }
bool IndexOpts::hasConfig() const noexcept { return !config.empty(); }
CollateMode IndexOpts::GetCollateMode() const noexcept { return static_cast<CollateMode>(collateOpts_.mode); }

IndexOpts& IndexOpts::PK(bool value) noexcept {
	options = value ? options | kIndexOptPK : options & ~(kIndexOptPK);
	return *this;
}

IndexOpts& IndexOpts::Array(bool value) noexcept {
	options = value ? options | kIndexOptArray : options & ~(kIndexOptArray);
	return *this;
}

IndexOpts& IndexOpts::Dense(bool value) noexcept {
	options = value ? options | kIndexOptDense : options & ~(kIndexOptDense);
	return *this;
}

IndexOpts& IndexOpts::Sparse(bool value) noexcept {
	options = value ? options | kIndexOptSparse : options & ~(kIndexOptSparse);
	return *this;
}

IndexOpts& IndexOpts::RTreeLinear(bool value) noexcept {
	options = value ? options | kIndexOptRTreeLinear : options & ~(kIndexOptRTreeLinear);
	return *this;
}

IndexOpts& IndexOpts::SetCollateMode(CollateMode mode) noexcept {
	collateOpts_.mode = mode;
	return *this;
}

IndexOpts& IndexOpts::SetConfig(const std::string& newConfig) {
	config = newConfig;
	return *this;
}
