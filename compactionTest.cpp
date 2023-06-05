//
// Created by 15006 on 2023/5/27.
//
#include <iostream>
#include <chrono>
#include <vector>
#include <fstream>
#include <ctime>
#include <cmath>
#include "KVStore.h"

using namespace std;
using namespace std::chrono;

void runPutTest(KVStore& kvStore, const vector<pair<uint64_t, string>>& testData) {
    ofstream file("compaction_data.txt", ios::out);

    int count = 0;
    double throughput = 0.0;  // 初始吞吐量为零

    for (const auto& data : testData) {
        auto start = high_resolution_clock::now();

        kvStore.put(data.first, data.second);
        count++;

        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        double elapsedTime = duration.count() / 1000.0;
        throughput = 1 / (elapsedTime);  // 计算吞吐量

        cout << count << " " << throughput << endl;
        if (std::isinf(throughput)) {
            file << 1000 << endl;  // 写入最大值
        } else {
            file << throughput << endl;  // 写入实际值
        }
    }

    file.close();
}


int main() {
    // 创建KVStore对象并选择存储目录
    KVStore kvStore("path/to/directory");

    // 生成测试数据
    vector<pair<uint64_t, string>> testData;
    // 构造测试数据
    for (uint64_t i = 1; i <= 32; i++) {
        string value(1024*512, 'A');  // 值占用更大空间
        testData.emplace_back(i, value);
    }


    // 运行测试并统计吞吐量
    runPutTest(kvStore, testData);


    return 0;
}
