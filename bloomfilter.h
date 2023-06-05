//
// Created by 15006 on 2023/5/6.
//

#ifndef LSM_KV_BLOOMFLITER_H
#define LSM_KV_BLOOMFLITER_H

#include <bitset>
#include "MurmurHash3.h"


#define BLOOMFILTER_SIZE (10240*8)

class BloomFilter {
private:
    std::bitset<BLOOMFILTER_SIZE> *bitmap;

public:
    BloomFilter();
    ~BloomFilter();

    void insert(const uint64_t key);
    bool find(const uint64_t key);
    std::bitset<BLOOMFILTER_SIZE> *getBitMapBuf();
    void loadBloomFilter(char* buf){
        memcpy((char*)bitmap, buf, BLOOMFILTER_SIZE/8);
    }

    void saveBloomFilter(char *buf) {
        memcpy(buf, (char*)bitmap, BLOOMFILTER_SIZE/8);
    }
};


#endif //LSM_KV_BLOOMFLITER_H
