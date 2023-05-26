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
    if(key < min_key || key > max_key) return "";
    if(!bloomFilter->find(key)) return "";

    bool isIn = false;
    int low = 0, high = index_area.size()-1;
    int mid;
    while(low <= high){
        mid = low + (high-low)/2;
        if(index_area[mid].getKey() < key){
            low = mid + 1;
        }else if(index_area[mid].getKey() > key){
            high = mid - 1;
        } else {
            isIn = true;
            break;
        }
    }
    if(!isIn) return "";

    ifstream file;
    file.open(file_path,ios::in|ios::binary);
    if(!file.is_open()){
        return "";
    }

    string res;

    cout << "read file:" << file_path <<endl;

    //the last index_item
    if(mid == index_area.size()-1){
        file.seekg(0, ios::end);                      // 设置指针到文件流尾部
        streampos ps = file.tellg();                  // 指针距离文件头部的距离，即为文件流大小
        uint64_t fileSize = ps;
        uint32_t size = fileSize - index_area[mid].getOffset();

        file.seekg(index_area[mid].getOffset());
        char *buf = new char[size+1];
        buf[size] = '\0';
        file.read(buf,size);
        res = buf;
        delete[] buf;
    } else{
        int size = index_area[mid+1].getOffset() - index_area[mid].getOffset();

        file.seekg(index_area[mid].getOffset());
        char *buf = new char[size+1];
        buf[size] = '\0';
        file.read(buf,size);
        res = buf;
        delete[] buf;
    }
    file.close();

//    cout << "get value size (from sstable): " << res.size() <<endl;
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
//    cout << "-----------------------------------------" << endl;
    cout << "write file:" << file_path <<endl;

    //head
    file.write(to_string(time_stamp).c_str(),8);
    file.write(to_string(pair_num).c_str(),8);
    file.write(to_string(min_key).c_str(),8);
    file.write(to_string(max_key).c_str(),8);

//    file.seekp(0, ios::end);                      // 设置指针到文件流尾部
//    streampos ps = file.tellp();                  // 指针距离文件头部的距离，即为文件流大小
//    uint64_t fileSize = ps;
//    cout << "after head, file size: " << fileSize <<endl;

    //bloomfilter
    Node<uint64_t,string> *cur = mem_table->head->next[0];
    while(cur != nullptr){
        sstable_new->bloomFilter->insert(cur->getKey());
        cur = cur->next[0];
    }
    std::bitset<BLOOMFILTER_SIZE> *filter_buf = sstable_new->bloomFilter->getBitMapBuf();
    file.write(filter_buf->to_string().c_str(),BLOOMFILTER_SIZE/8);

//    file.seekp(0, ios::end);                      // 设置指针到文件流尾部
//    ps = file.tellp();                  // 指针距离文件头部的距离，即为文件流大小
//    fileSize = ps;
//    cout << "after bloomfilter, file size: " << fileSize <<endl;

    //index_area
    uint32_t offset = 10272 + 12*pair_num;
    cur = mem_table->head->next[0];
    while(cur != nullptr){
        sstable_new->index_area.emplace_back(cur->getKey(),offset);

        file.write(to_string(cur->getKey()).c_str(),8);
        file.write(to_string(offset).c_str(),4);

//        //TODO: delete the debug code
//        cout << "----------------------" << endl;
//        cout << "write key:" << cur->getKey() << " offset:" << offset <<endl;
//        cout << "size of value: " << cur->getValue().size() <<endl;
//        bool isAlls = true;
//        for(auto &i:cur->getValue()){
//            if(i != 's'){
//                isAlls = false;
//                break;
//            }
//        }
//        cout << "isAlls: " << isAlls <<endl;

        offset += cur->getValue().size();
        cur = cur->next[0];
    }

//    file.seekp(0, ios::end);                      // 设置指针到文件流尾部
//    ps = file.tellp();                  // 指针距离文件头部的距离，即为文件流大小
//    fileSize = ps;
//    cout << "after index_area, file size: " << fileSize <<endl;

    //data_area
    cur = mem_table->head->next[0];
    while(cur != nullptr){
        file.write(cur->getValue().c_str(),static_cast<std::streamsize>(cur->getValue().size()));
        cur = cur->next[0];
    }

//    file.seekp(0, ios::end);                      // 设置指针到文件流尾部
//    streampos ps = file.tellp();                  // 指针距离文件头部的距离，即为文件流大小
//    uint64_t fileSize = ps;
//    cout << "file size: " << fileSize <<endl;

//    file.seekp(0, ios::end);                      // 设置指针到文件流尾部
//    ps = file.tellp();                  // 指针距离文件头部的距离，即为文件流大小
//    fileSize = ps;
//    cout << "after data_area, file size: " << fileSize <<endl;

    file.close();
    return sstable_new;
}

SStable *SStable::read_sstable_from_disk(const string &file_path) {
    ifstream file;
    file.open(file_path, ios::in|ios::binary);
    if(!file.is_open()){
        return nullptr;
    }

    /*Header*/
    char *buf = new char[9];
    buf[8] = '\0';

    file.read(buf,8);
    uint64_t time_stamp = stoull(buf);
    file.read(buf,8);
    uint64_t pair_num = stoull(buf);
    file.read(buf,8);
    uint64_t min_key = stoull(buf);
    file.read(buf,8);
    uint64_t max_key = stoull(buf);

    auto *sstable_new = new SStable(time_stamp,pair_num,min_key,max_key);

    /*BloomFilter*/
    file.read(reinterpret_cast<char*>(sstable_new->bloomFilter->getBitMapBuf()),BLOOMFILTER_SIZE/8);

    /*Index_area*/
    for(int i=0;i<pair_num;i++){
        uint64_t key;
        uint32_t offset;
        file.read((char*)&key, 8);
        file.read((char*)&offset, 4);
        sstable_new->index_area.emplace_back(key,offset);
    }
    file.close();

    return sstable_new;
}
