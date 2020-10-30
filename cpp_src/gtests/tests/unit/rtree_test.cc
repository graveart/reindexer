#include "core/index/rtree/rtree.h"
#include <random>
#include "core/index/rtree/linearsplitter.h"
#include "core/index/rtree/quadraticsplitter.h"
#include "gtest/gtest.h"

namespace {
static double randDouble(double min, double max) {
	using dist_type = std::uniform_real_distribution<double>;
	thread_local static std::mt19937 gen(std::random_device{}());
	thread_local static dist_type dist;

	return dist(gen, dist_type::param_type{min, max});
}

static reindexer::Point randPoint() {
	static constexpr double range = 1000.0;
	return {randDouble(-range, range), randDouble(-range, range)};
}

template <typename T>
struct Compare;

template <>
struct Compare<reindexer::Point> {
	bool operator()(reindexer::Point lhs, reindexer::Point rhs) const noexcept {
		if (lhs.x == rhs.x) return lhs.y < rhs.y;
		return lhs.x < rhs.x;
	}
};

template <typename T>
struct Compare<reindexer::RMapValue<T, size_t>> {
	bool operator()(const reindexer::RMapValue<T, size_t>& lhs, const reindexer::RMapValue<T, size_t>& rhs) const noexcept {
		return lhs.second < rhs.second;
	}
};

template <typename RTree>
class SearchVisitor : public RTree::Visitor {
public:
	bool operator()(const typename RTree::value_type& v) override {
		const auto it = data_.find(v);
		if (it == data_.end()) {
			++wrong_;
		} else {
			data_.erase(it);
		}
		return false;
	}
	size_t Size() const noexcept { return data_.size(); }
	void Add(const typename RTree::value_type& r) { data_.insert(r); }
	size_t Wrong() const noexcept { return wrong_; }

private:
	size_t wrong_ = 0;
	std::multiset<typename RTree::value_type, Compare<typename RTree::value_type>> data_;
};

template <typename RTree>
class DeleteVisitor : public RTree::Visitor {
public:
	DeleteVisitor(const reindexer::Rectangle& r) : rect_{r} {}
	bool operator()(const typename RTree::value_type& v) override { return rect_.Contain(RTree::traits::GetPoint(v)); }

private:
	const reindexer::Rectangle rect_;
};

}  // namespace

// Checks of inserting of points to RectangleTree and verifies of its structure after each insertion
template <template <typename, typename, typename, typename, size_t> class Splitter>
static void TestInsert() {
	reindexer::RectangleTree<reindexer::Point, Splitter> tree;
	ASSERT_TRUE(tree.Check());

	size_t insertedCount = 0;
	for (size_t i = 0; i < 10000; ++i) {
		const auto p = randPoint();
		const auto insertRes{tree.insert(reindexer::Point{p})};
		if (insertRes.second) {
			++insertedCount;
		}
		ASSERT_TRUE(*insertRes.first == p);
		ASSERT_TRUE(tree.Check());
		ASSERT_EQ(tree.size(), insertedCount);
	}
}

TEST(RTree, QuadraticInsert) { TestInsert<reindexer::QuadraticSplitter>(); }

TEST(RTree, LinearInsert) { TestInsert<reindexer::LinearSplitter>(); }

// Checks that iterators could iterate over whole RectangleTree after multiple modifications of the tree
template <template <typename, typename, typename, typename, size_t> class Splitter>
static void TestIterators() {
	reindexer::RectangleTree<reindexer::Point, Splitter> tree;
	ASSERT_TRUE(tree.Check());
	ASSERT_TRUE(tree.begin() == tree.end());
	ASSERT_FALSE(tree.begin() != tree.end());
	ASSERT_TRUE(tree.cbegin() == tree.cend());
	ASSERT_FALSE(tree.cbegin() != tree.cend());

	for (size_t i = 0; i < 10000; ++i) {
		tree.insert(randPoint());
		ASSERT_TRUE(tree.Check());
		auto it = tree.begin(), end = tree.end();
		auto cit = tree.cbegin(), cend = tree.cend();
		for (size_t j = 0; j <= i; ++j) {
			ASSERT_FALSE(it == end);
			ASSERT_TRUE(it != end);
			ASSERT_FALSE(cit == cend);
			ASSERT_TRUE(cit != cend);
			++it;
			++cit;
		}
		ASSERT_TRUE(it == end);
		ASSERT_FALSE(it != end);
		ASSERT_TRUE(cit == cend);
		ASSERT_FALSE(cit != cend);
	}
}

TEST(RTree, QuadraticIterators) { TestIterators<reindexer::QuadraticSplitter>(); }

TEST(RTree, LinearIterators) { TestIterators<reindexer::LinearSplitter>(); }

// Verifies of searching of points in RectangleTree by DWithin
template <template <typename, typename, typename, typename, size_t> class Splitter>
static void TestSearch() {
	using RTree = reindexer::RectangleTree<reindexer::Point, Splitter>;
	constexpr size_t kCount = 100000;

	RTree tree;
	std::vector<reindexer::Point> data;
	for (size_t i = 0; i < kCount; ++i) {
		auto p{randPoint()};
		data.push_back(p);
		tree.insert(std::move(p));
	}
	ASSERT_TRUE(tree.Check());
	ASSERT_EQ(tree.size(), kCount);

	for (size_t i = 0; i < 1000; ++i) {
		SearchVisitor<RTree> DWithinVisitor;
		const reindexer::Point point{randPoint()};
		const double distance = randDouble(0.0, 100.0);
		for (const auto& r : data) {
			if (reindexer::DWithin(point, r, distance)) DWithinVisitor.Add(r);
		}

		tree.DWithin(point, distance, DWithinVisitor);
		ASSERT_EQ(DWithinVisitor.Size(), 0);
		ASSERT_EQ(DWithinVisitor.Wrong(), 0);
	}
}

TEST(RTree, QuadraticSearch) { TestSearch<reindexer::QuadraticSplitter>(); }

TEST(RTree, LinearSearch) { TestSearch<reindexer::LinearSplitter>(); }

// Checks of deleting of points from RectangleTree and verifies of its structure after each deletion
template <template <typename, typename, typename, typename, size_t> class Splitter>
static void TestDelete() {
	using RTree = reindexer::RectangleTree<reindexer::Point, Splitter>;
	constexpr size_t kCount = 10000;

	RTree tree;
	for (size_t i = 0; i < kCount;) {
		i += tree.insert(randPoint()).second;
	}
	ASSERT_TRUE(tree.Check());
	ASSERT_EQ(tree.size(), kCount);

	size_t deletedCount = 0;
	for (size_t i = 0; i < 1000; ++i) {
		DeleteVisitor<RTree> visitor{{randPoint(), randPoint()}};
		if (tree.DeleteOneIf(visitor)) {
			++deletedCount;
		}
		ASSERT_TRUE(tree.Check());
		ASSERT_EQ(tree.size(), kCount - deletedCount);
	}
}

TEST(RTree, QuadraticDelete) { TestDelete<reindexer::QuadraticSplitter>(); }

TEST(RTree, LinearDelete) { TestDelete<reindexer::LinearSplitter>(); }

// Checks of deleting of points iterators point to from RectangleTree and verifies of its structure after each deletion
template <template <typename, typename, typename, typename, size_t> class Splitter>
static void TestErase() {
	using RTree = reindexer::RectangleTree<reindexer::Point, Splitter>;
	constexpr size_t kCount = 10000;

	RTree tree;
	for (size_t i = 0; i < kCount;) {
		i += tree.insert(randPoint()).second;
	}
	ASSERT_TRUE(tree.Check());
	ASSERT_EQ(tree.size(), kCount);

	for (size_t i = 0; i < 1000; ++i) {
		auto it = tree.begin();
		for (size_t j = 0, k = rand() % (kCount - i); j < k; ++j) {
			++it;
		}
		tree.erase(it);
		ASSERT_TRUE(tree.Check()) << i;
		ASSERT_EQ(tree.size(), kCount - i - 1);
	}
}

TEST(RTree, QuadraticErase) { TestErase<reindexer::QuadraticSplitter>(); }

TEST(RTree, LinearErase) { TestErase<reindexer::LinearSplitter>(); }

// Checks of inserting, deleting search of points in RectangleTree and verifies of its structure after each its modidfication
template <template <typename, typename, typename, typename, size_t> class Splitter>
static void TestMap() {
	using Map = reindexer::RTreeMap<size_t, Splitter>;
	constexpr size_t kCount = 10000;

	Map map;
	std::vector<typename Map::value_type> data;
	for (size_t i = 0; i < kCount; ++i) {
		auto p = randPoint();
		data.emplace_back(p, i);
		map.insert({std::move(p), i});
	}
	ASSERT_TRUE(map.Check());

	for (size_t i = 0; i < 1000; ++i) {
		SearchVisitor<Map> visitor;
		const reindexer::Point point{randPoint()};
		const double distance = randDouble(0.0, 100.0);
		for (const auto& r : data) {
			if (reindexer::DWithin(point, r.first, distance)) visitor.Add(r);
		}
		map.DWithin(point, distance, visitor);
		ASSERT_EQ(visitor.Size(), 0);
		ASSERT_EQ(visitor.Wrong(), 0);
	}

	size_t deletedCount = 0;
	for (size_t i = 0; i < 1000; ++i) {
		DeleteVisitor<Map> visitor{{randPoint(), randPoint()}};
		ASSERT_TRUE(map.Check());
		if (map.DeleteOneIf(visitor)) {
			++deletedCount;
		}
		ASSERT_EQ(map.size(), kCount - deletedCount);
	}
}

TEST(RTree, QuadraticMap) { TestMap<reindexer::QuadraticSplitter>(); }

TEST(RTree, LinearMap) { TestMap<reindexer::LinearSplitter>(); }
