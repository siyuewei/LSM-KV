//
// Created by 15006 on 2023/5/6.
//

#ifndef LSM_KV_SSTABLE_H
#define LSM_KV_SSTABLE_H

#include <cstdint>
#include "bloomfilter.h"
#include <vector>
#include "skipList.h"
#include "utils.h"
#include "fstream"

using namespace std;

struct index_item{
private:
    uint64_t key;
    uint32_t offset;

public:
    index_item(uint64_t key, uint32_t offset):key(key),offset(offset){}
    uint64_t getKey(){return key;}
    uint32_t getOffset(){return offset;}
};


class SStable {
private:
    /*Header*/
    uint64_t time_stamp;
    uint64_t pair_num;
    uint64_t min_key, max_key;


//    vector<string> data_area;

public:
    BloomFilter *bloomFilter;
    vector<index_item> index_area;
    SStable(uint64_t t_stamp, uint64_t p_num, uint64_t min_k, uint64_t max_k);
//    void set_index_area(uint64_t key, uint32_t value);
//    void set_data_area(string data);

    //basic function
    string search(uint64_t key, const string& dir_path);

    //DiskStorage
    static SStable* memtable_to_sstable(const string& file_path, SkipList<uint64_t,std::string>*mem_table, uint64_t time_stamp, uint64_t tag);

};


#endif //LSM_KV_SSTABLE_H
