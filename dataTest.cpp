#include <iostream>
#include <chrono>
#include <list>
#include "KVStore.h"

using namespace std;
using namespace std::chrono;

void runPutTest(KVStore& kvStore, const vector<pair<uint64_t, string>>& testData) {
    auto start = high_resolution_clock::now();

    for (const auto& data : testData) {
        kvStore.put(data.first, data.second);
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    cout << "Put Test:" << endl;
    cout << "Total Time: " << double(duration.count()) << " ms" << endl;
    cout << "Average Latency: " << double(duration.count()) / double(testData.size()) << " ms" << endl;
    cout << "Throughput: " << double(testData.size()) / (duration.count() / 1000.0) << " ops/s" << endl;
}

void runGetTest(KVStore& kvStore, const vector<pair<uint64_t, string>>& testData) {
    auto start = high_resolution_clock::now();

    for (const auto& data : testData) {
        kvStore.get(data.first);
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    cout << "Get Test:" << endl;
    cout << "Total Time: " << double(duration.count()) << " ms" << endl;
    cout << "Average Latency: " << double(duration.count()) / double(testData.size()) << " ms" << endl;
    cout << "Throughput: " << double(testData.size()) / (duration.count() / 1000.0) << " ops/s" << endl;
}

void runDeleteTest(KVStore& kvStore, const vector<pair<uint64_t, string>>& testData) {
    auto start = high_resolution_clock::now();

    // Delete all data
    for (const auto& data : testData) {
        kvStore.del(data.first);
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    cout << "Delete Test:" << endl;
    cout << "Total Time: " << double(duration.count()) << " ms" << endl;
    cout << "Average Latency: " << double(duration.count()) / double(testData.size()) << " ms" << endl;
    cout << "Throughput: " << double(testData.size()) / (duration.count() / 1000.0) << " ops/s" << endl;
}

void runGetWithoutBloomTest(KVStore& kvStore, const vector<pair<uint64_t, string>>& testData) {
    auto start = high_resolution_clock::now();

    for (const auto& data : testData) {
        kvStore.get_without_bloom(data.first);
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    cout << "GetWithoutBloom Test:" << endl;
    cout << "Total Time: " << double(duration.count()) << " ms" << endl;
    cout << "Average Latency: " << double(duration.count()) / double(testData.size()) << " ms" << endl;
    cout << "Throughput: " << double(testData.size()) / (duration.count() / 1000.0) << " ops/s" << endl;
}

void runGetWithoutSStableTest(KVStore& kvStore, const vector<pair<uint64_t, string>>& testData) {
    auto start = high_resolution_clock::now();

    // Delete all data
    for (const auto& data : testData) {
        kvStore.get_without_sstables(data.first);
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    cout << "GetWithoutSStable Test:" << endl;
    cout << "Total Time: " << double(duration.count()) << " ms" << endl;
    cout << "Average Latency: " << double(duration.count()) / double(testData.size()) << " ms" << endl;
    cout << "Throughput: " << double(testData.size()) / (duration.count() / 1000.0) << " ops/s" << endl;
}

int main() {
    // 创建KVStore对象并选择存储目录
    KVStore kvStore("./testData");

    // 第一个测试集：32个键值对，每个value是1024*512个byte
    vector<pair<uint64_t, string>> testData1;
    // 构造测试数据
    for (uint64_t i = 1; i <= 32; i++) {
        string value(1024*512, 'A');
        testData1.emplace_back(i, value);
    }

    // 第二个测试集：1024个键值对，每个value是32*512个byte
    vector<pair<uint64_t, string>> testData2;
    // 构造测试数据
    for (uint64_t i = 1; i <= 1024; i++) {
        string value(32*512, 'B');
        testData2.emplace_back(i, value);
    }

    // 第三个测试集：1024*32个键值对，每个value是512个byte
    vector<pair<uint64_t, string>> testData3;
    // 构造测试数据
    for (uint64_t i = 1; i <= 1024 * 32; i++) {
        string value(512, 'C');
        testData3.emplace_back(i, value);
    }

    // 运行测试
    kvStore.reset();

    runPutTest(kvStore, testData1);
    runGetTest(kvStore,testData1 );
    runGetWithoutBloomTest(kvStore,testData1);
    runGetWithoutSStableTest(kvStore,testData1);
    runDeleteTest(kvStore,testData1 );
    kvStore.reset();

    runPutTest(kvStore, testData2);
    runGetTest(kvStore,testData2 );
    runGetWithoutBloomTest(kvStore,testData2);
    runGetWithoutSStableTest(kvStore,testData2);
    runDeleteTest(kvStore,testData2 );
    kvStore.reset();

    runPutTest(kvStore, testData3);
    runGetTest(kvStore,testData3 );
    runGetWithoutBloomTest(kvStore,testData3);
    runGetWithoutSStableTest(kvStore,testData3);
    runDeleteTest(kvStore,testData3 );
    kvStore.reset();

    return 0;
}
