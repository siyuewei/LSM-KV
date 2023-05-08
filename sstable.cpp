//
// Created by 15006 on 2023/5/6.
//

#include "sstable.h"
SStable::SStable(uint64_t t_stamp, uint64_t p_num, uint64_t min_k, uint64_t max_k)
:time_stamp(t_stamp), pair_num(p_num), min_key(min_k), max_key(max_k){
    bloomFilter = new BloomFilter();
}

//void sstable::set_index_area(uint64_t key, uint32_t value) {
//    index_area.emplace_back(key,value);
//}

//void sstable::set_data_area(string data) {
//    data_area.emplace_back(data);
//}
string SStable::search(uint64_t key, const string&file_path) {
    if(!bloomFilter->find(key)) return "";

    bool isIn = false;
    int low = 0, high = index_area.size()-1;
    int mid;
    while(low <= high){
        mid = low + (high-low)/2;
        if(index_area[mid].getKey() < key){
            low = mid;
        }else if(index_area[mid].getKey() > key){
            high = mid;
        } else {
            isIn = true;
            break;
        }
    }
    if(!isIn) return "";

    ifstream file;
    file.open(file_path,ios::in);
    file.seekg(index_area[mid].getOffset());
    string res;

    //the last index_item
    if(mid == index_area.size()-1){
        int size = sizeof(file) - index_area[mid].getOffset();
        char *buf = new char[size];
        file.read(buf,size);
        res = buf;
        delete[] buf;
    } else{
        int size = index_area[mid+1].getOffset() - index_area[mid].getOffset();
        char *buf = new char[size];
        file.read(buf,size);
        res = buf;
        delete[] buf;
    }
    return res;
}



SStable *SStable::memtable_to_sstable(const string& dir_path, SkipList<uint64_t, std::string> *mem_table, uint64_t time_stamp, uint64_t tag) {
    uint64_t pair_num = mem_table->getItemNum();
    uint64_t min_key = mem_table->getMinKey();
    uint64_t max_key = mem_table->getMaxKey();
    auto *sstable_new = new SStable(time_stamp,pair_num,min_key,max_key);

    if(!utils::dirExists(dir_path)){
        utils::mkdir(dir_path.c_str());
    }

    const string file_path = dir_path + "/" + to_string(time_stamp) + "_" + to_string(tag) + ".ssh";
    ofstream file;
    file.open(file_path, ios::out|ios::binary);

    if(!file.is_open()){
        return nullptr;
    }

    file.seekp(0);

    file.write(to_string(time_stamp).c_str(),8);
    file.write(to_string(pair_num).c_str(),8);
    file.write(to_string(min_key).c_str(),8);
    file.write(to_string(max_key).c_str(),8);

//    FINISH:file.write bloomfilter
    Node<uint64_t,string> *cur = mem_table->head->next[0];
    while(cur->next[0] != nullptr){
        sstable_new->bloomFilter->insert(cur->getKey());
        cur = cur->next[0];
    }
    std::bitset<BLOOMFILTER_SIZE> *filter_buf = sstable_new->bloomFilter->getBitMapBuf();
    file.write(filter_buf->to_string().c_str(),BLOOMFILTER_SIZE/8);

    //index_area
    uint32_t offset = 10272 + 12*pair_num;
    cur = mem_table->head->next[0];
    while(cur->next[0] != nullptr){
        sstable_new->index_area.emplace_back(cur->getKey(),offset);

        file.write(to_string(cur->getKey()).c_str(),8);
        file.write(to_string(offset).c_str(),4);
        offset += cur->getValue().size();
        cur = cur->next[0];
    }

    //data_area
    cur = mem_table->head->next[0];
    while(cur->next[0] != nullptr){
        file.write(cur->getValue().c_str(),static_cast<std::streamsize>(cur->getValue().size()));
        cur = cur->next[0];
    }

    return sstable_new;
}