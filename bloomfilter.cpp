//
// Created by 15006 on 2023/5/6.
//

#include "bloomfilter.h"
BloomFilter::BloomFilter() {
    bitmap = new std::bitset<BLOOMFILTER_SIZE>;
    bitmap->reset();
}

BloomFilter::~BloomFilter() {

}

void BloomFilter::insert(const uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key,sizeof(key),1,&hash);

    for(auto &hash_num : hash){
        bitmap->set(hash_num%BLOOMFILTER_SIZE);
    }
}

bool BloomFilter::find(const uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key,sizeof(key),1,&hash);

    for(auto &hash_num : hash){
        if(bitmap->test(hash_num%BLOOMFILTER_SIZE) == 0) return false;
    }
    return true;
}

std::bitset<BLOOMFILTER_SIZE> *BloomFilter::getBitMapBuf() {
//    std::bitset<BLOOMFILTER_SIZE> *filterBuf = new std::bitset<BLOOMFILTER_SIZE>(bitmap);
//    return filterBuf;

    return bitmap;
}

