//
// Created by 15006 on 2023/5/27.
//
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

int main() {
    // 创建KVStore对象并选择存储目录
    KVStore kvStore("./testData");

    // 1024*32个键值对，每个value是512个byte
    vector<pair<uint64_t, string>> testData;
    // 构造测试数据
    for (uint64_t i = 1; i <= 1024*32; i++) {
        string value(512, 'B');
        testData.emplace_back(i, value);
    }


    // 运行测试
    kvStore.reset();
    runPutTest(kvStore,testData);
    runGetTest(kvStore,testData);
    kvStore.reset();


    return 0;
}
