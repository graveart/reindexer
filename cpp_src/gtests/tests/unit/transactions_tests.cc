#include <condition_variable>
#include "transaction_api.h"

TEST_F(TransactionApi, ConcurrencyTest) {
	const size_t smallPortion = 100;
	const size_t mediumPortion = 1000;
	const size_t bigPortion = 15000;
	const size_t hugePortion = 30000;

	std::array<DataRange, 5> ranges = {{{0, 1000, "initial"},
										{1000, 49000, "first_writer"},
										{49000, 55000, "second_writer"},
										{55000, 130000, "third_writer"},
										{130000, 190000, "fourth_writer"}}};

	std::condition_variable cond;
	std::mutex mtx;

	AddDataToNsTx(ranges[0].from, ranges[0].till, ranges[0].data);
	std::vector<std::thread> readThreads;
	std::vector<std::thread> writeThreads;
	std::atomic<bool> stop{false};

	readThreads.emplace_back(std::thread([&] {
		while (!stop.load()) {
			SelectData(0, GetItemsCount());
		}
	}));
	readThreads.emplace_back(std::thread([&] {
		while (!stop.load()) {
			SelectData(0, GetItemsCount());
		}
	}));

	writeThreads.emplace_back(std::thread(
		[&](int id) {
			std::unique_lock<std::mutex> lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			for (size_t i = 0; i < 3; ++i) {
				size_t portion = GetPortion(from, bigPortion, ranges[id].till);
				AddDataToNsTx(from, bigPortion, ranges[id].data);
				from += portion;
				std::this_thread::yield();
				portion = GetPortion(from, mediumPortion, ranges[id].till);
				AddDataToNsTx(from, mediumPortion, ranges[id].data);
				from += portion;
				std::this_thread::yield();
			}
		},
		1));
	writeThreads.emplace_back(std::thread(
		[&](int id) {
			std::unique_lock<std::mutex> lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			size_t till = ranges[id].till;
			size_t portion = 0;
			for (size_t pos = from; pos < till; pos += portion) {
				portion = GetPortion(from, smallPortion, till);
				AddDataToNsTx(pos, smallPortion, ranges[id].data);
				std::this_thread::yield();
			}
		},
		2));
	writeThreads.emplace_back(std::thread(
		[&](int id) {
			std::unique_lock<std::mutex> lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			for (size_t i = 0; i < 5; ++i) {
				size_t portion = GetPortion(from, bigPortion, ranges[id].till);
				AddDataToNsTx(from, portion, ranges[id].data);
				from += portion;
				std::this_thread::yield();
			}
		},
		3));
	writeThreads.emplace_back(std::thread(
		[&](int id) {
			std::unique_lock<std::mutex> lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			for (size_t i = 0; i < 2; ++i) {
				size_t portion = GetPortion(from, hugePortion, ranges[id].till);
				AddDataToNsTx(from, portion, ranges[id].data);
				from += portion;
				std::this_thread::yield();
			}
		},
		4));

	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	cond.notify_all();
	for (auto& it : writeThreads) {
		it.join();
	}
	stop = true;
	for (auto& it : readThreads) {
		it.join();
	}

	QueryResults qr;
	Error err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), ranges.back().till);
	for (auto it : qr) {
		auto item = it.GetItem();
		int id = static_cast<size_t>(item[kFieldId].As<int>());
		auto data = item[kFieldData].As<std::string>();
		bool idIsCorrect = false;
		for (auto& it : ranges) {
			if (id >= it.from && id < it.till) {
				ASSERT_EQ(data, it.data + "_" + std::to_string(id));
				idIsCorrect = true;
				break;
			}
		}
		ASSERT_TRUE(idIsCorrect);
	}
}
