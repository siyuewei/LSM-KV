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

    //TODO:ADD THIS
//    cout << "read file:" << file_path <<endl;

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
//TODO:ADD THIS
//    cout << "write file:" << file_path <<endl;

    //head
    file.write((const char*)&time_stamp, 8);
    file.write((const char*)&pair_num, 8);
    file.write((const char*)&min_key, 8);
    file.write((const char*)&max_key, 8);

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
//    std::bitset<BLOOMFILTER_SIZE> *filter_buf = sstable_new->bloomFilter->getBitMapBuf();
//    file.write(filter_buf->to_string().c_str(),BLOOMFILTER_SIZE/8);
    char* bloom_buffer = new char[10240];
    sstable_new->bloomFilter->saveBloomFilter(bloom_buffer);
    file.write(bloom_buffer, 10240);
    delete[] bloom_buffer;

//    file.seekp(0, ios::end);                      // 设置指针到文件流尾部
//    ps = file.tellp();                  // 指针距离文件头部的距离，即为文件流大小
//    fileSize = ps;
//    cout << "after bloomfilter, file size: " << fileSize <<endl;

    //index_area
    uint32_t offset = 10272 + 12*pair_num;
    cur = mem_table->head->next[0];
    while(cur != nullptr){
        sstable_new->index_area.emplace_back(cur->getKey(),offset);

//        file.write(to_string(cur->getKey()).c_str(),8);
//        file.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
        uint64_t cur_key = cur->getKey();
        file.write((const char*)&cur_key, 8);
        file.write((const char*)&offset, 4);


//        cout << "----------------------" << endl;
//        cout << "write key:" << cur->getKey() << " offset:" << offset <<endl;
//        cout << "size of value: " << cur->getValue().size() <<endl;

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

//    read_sstable_from_disk(file_path);
    return sstable_new;
}

SStable *SStable::read_sstable_from_disk(const string &file_path) {
    ifstream file;
    file.open(file_path, ios::in|ios::binary);
    if(!file.is_open()){
        return nullptr;
    }

    /*Header*/
//    char *buf = new char[9];
//    buf[8] = '\0';
//
//    file.read(buf,8);
//    uint64_t time_stamp = stoull(buf);
//    file.read(buf,8);
//    uint64_t pair_num = stoull(buf);
//    file.read(buf,8);
//    uint64_t min_key = stoull(buf);
//    file.read(buf,8);
//    uint64_t max_key = stoull(buf);
    uint64_t time_stamp_cur, pair_num, min_key, max_key;
    file.read((char*)&time_stamp_cur, 8);
    file.read((char*)&pair_num, 8);
    file.read((char*)&min_key, 8);
    file.read((char*)&max_key, 8);

    auto *sstable_new = new SStable(time_stamp_cur,pair_num,min_key,max_key);

    /*BloomFilter*/
    char* bloom_buffer = new char[BLOOMFILTER_SIZE/8];
    file.read(bloom_buffer,BLOOMFILTER_SIZE/8);
    sstable_new->bloomFilter->loadBloomFilter(bloom_buffer);

    /*Index_area*/
//    char key_buf[9];
//    char offset_buf[5];
    for(int i=0;i<pair_num;i++){
//        file.read(key_buf, 8);
//        file.read(offset_buf, 4);
//        key_buf[8] = '\0';
//        offset_buf[4] = '\0';
        uint64_t key;
        uint32_t offset;
        file.read((char*)&key, 8);
        file.read((char*)&offset, 4);
        sstable_new->index_area.emplace_back(key,offset);
    }

//    //debug
//    string value = "";
//    uint64_t key;
//    uint32_t next_offset;
//    uint32_t this_offset;
//
//    for(int i = 0; i < sstable_new->index_area.size(); ++i){
//        auto this_item = sstable_new->index_area[i];
//        key = this_item.getKey();
//        this_offset = this_item.getOffset();
//
////        if(i != sstable_new->index_area.size()-1){
////            auto next_item = sstable_new->index_area[i+1];
////            next_offset = next_item.getOffset();
////        }else{
////            cout << "Read key :" << key << " ";
////            continue;
////        }
////
////        uint32_t size = next_offset - this_offset;
////        char buf[size+1];
////        buf[size]='\0';
////        file.read(buf,size);
////        value = string(buf,size);
//        cout << "Read key :" << key << " ";
//    }
//    file.close();

    return sstable_new;
}

string SStable::serach_without_bloom(uint64_t key, const string&file_path) {
    if(key < min_key || key > max_key) return "";

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

    //TODO:ADD THIS
//    cout << "read file:" << file_path <<endl;

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
