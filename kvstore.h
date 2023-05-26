#pragma once

#include <map>
#include "kvstore_api.h"
#include "skipList.h"
#include "sstable.h"
#include "diskstorage.h"
#include <string>
#include <iostream>
#include <fstream>

using namespace std;

//TODO: remember to change the size to 2MB(2*1024*1024)
#define MEMTABLE_MAXSIZE (2*1024*1024)
/*MODE*/
enum mode{
    Tiering,
    Leveling
};


class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    /*DRAM*/
    SkipList<uint64_t,std::string>* mem_table;
    uint64_t time_stamp;
    uint64_t tag;
    string dir;

    /*DISK*/
    //ensure the every level of sstables can sort by <time_stamp,min_key> auto
    map<uint64_t,map<pair<uint64_t,uint64_t>,SStable*>> sstables; //level, <time_stamp,tag>, sstable

    void read_from_disk();

//    void get_header(const string &file_path,uint64_t &time_stamp,uint64_t &pair_num,uint64_t &min_key,uint64_t &max_key);

    map<uint64_t,pair<uint64_t,mode>> level_config; //level, <max_num,mode>

    void read_config();

    void read_data(const string& file_path,map<uint64_t,pair<string,uint64_t>> *data,bool isLast);
    void delete_sstables_file(const string& file_path);
    uint64_t get_time_stamp(const string& file_path);

public:


	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

    void compaction(uint64_t level);
};
