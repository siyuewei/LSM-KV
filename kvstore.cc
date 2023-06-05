#include "kvstore.h"

//int arr[10440] = {0};

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    this->dir = dir;
    mem_table = new SkipList<uint64_t,string>();
    sstables = {{0,{}}};
    time_stamp = 1;
    tag = 1;
    read_config();
    read_from_disk();
}

KVStore::~KVStore()
{
    if(mem_table && mem_table->head && mem_table->head->next[0]){
        const string dir_path = dir + "/level-0";
        SStable *sstable_new = SStable::memtable_to_sstable(dir_path,mem_table,time_stamp,tag);
        sstables[0].insert({{time_stamp,tag},sstable_new});
        ++this->time_stamp;
        ++this->tag;
        compaction(0);
        delete mem_table;

//        sstables[0].insert({{time_stamp,tag},sstable_new});
//        ++time_stamp;
//        ++tag;
    }
//    read_from_disk();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    //FINISH : check if the sstable size will larger than 2MB
    //FINISH: check if the put is a update
    if(mem_table->getMemSize() + 12 + s.size() <= MEMTABLE_MAXSIZE){
        mem_table->insert(key, s);
        return;
    }

    //size > 2MB, create sstable
    //FINISH: NOW IN LEVEL 0
    const string dir_path = dir + "/level-0";
    SStable *sstable_new = SStable::memtable_to_sstable(dir_path,mem_table,time_stamp,tag);
    mem_table->clear();

    sstables[0].insert({{time_stamp,tag},sstable_new});
//    sstables.find(0)->second.insert({{time_stamp,tag},sstable_new});
    ++this->time_stamp;
    ++this->tag;

    mem_table->insert(key,s);//Don't forget to insert the new key-value pair
    compaction(0);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string value = mem_table->serach(key);
    if(value == "~DELETED~") return "";
    if(value != "") return value;

    string value_possible = "";
    uint64_t time_stamp_max = 0;
    uint64_t tag_max = 0;
    //FINISH:handle the "~DELETED~" in sstable
    for(auto &sstable_level_time_tag : sstables){
        for(auto &sstable_time_tag: sstable_level_time_tag.second){
            SStable* sstable_cur = sstable_time_tag.second;
            const string file_path = dir + "/level-" + to_string(sstable_level_time_tag.first) + "/" + to_string(sstable_time_tag.first.first) + "_" + to_string(sstable_time_tag.first.second) + ".ssh";
            if(!(value = sstable_cur->search(key,file_path)).empty()){
//                cout << "the value size return form KVStore::get is " << value.size() << endl; //FINISH:delete this line
//                if(value == "~DELETED~") return "";
//                return value;
                if((sstable_time_tag.first.first > time_stamp_max) || (sstable_time_tag.first.first == time_stamp_max && sstable_time_tag.first.second > tag_max)){
                    value_possible = value;
                    time_stamp_max = sstable_time_tag.first.first;
                    tag_max = sstable_time_tag.first.second;
                }
            }
        }
    }
    if(value_possible == "~DELETED~") return "";
    if(value_possible != "") return value_possible;
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    string value = get(key);
    if(value.empty()) return false;

    mem_table->insert(key,"~DELETED~");
	return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    /*mem_table*/
    mem_table->clear();

    /*sstable_file*/
//    for(auto &sstable_level_time_tag : sstables){
//        for(auto &sstable_time_tag: sstable_level_time_tag.second){
//            SStable* sstable_cur = sstable_time_tag.second;
//            const string file_path = dir + "level-" + to_string(sstable_level_time_tag.first) + "/" + to_string(sstable_time_tag.first.first) + "_" + to_string(sstable_time_tag.first.second);
//            utils::rmfile(file_path.c_str());
//        }
//        const string dir_path = dir + "level-" + to_string(sstable_level_time_tag.first);
//        utils::rmdir(dir_path.c_str());
//    }
//    utils::rmdir(dir.c_str());
    vector<string> level_list;
    utils::scanDir(this->dir,level_list);
    for(auto &level_name:level_list){
        string level_path = this->dir + "/" + level_name;
        vector<string> file_list;
        utils::scanDir(level_path,file_list);
        for(auto &file_name:file_list){
            string file_path = level_path + "/" + file_name;
            utils::rmfile(file_path.c_str());
        }
        utils::rmdir(level_path.c_str());
    }

    /*sstables*/
    sstables.clear();

    /*others*/
    this->time_stamp = 0;
    this->tag = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}

void KVStore::compaction(uint64_t level) {
    auto it = level_config.find(level);
    if(it == level_config.end()){
        uint64_t num = level_config.find(level-1)->second.first * 2;
        level_config.insert({level,{num,Leveling}});
    }
    if(sstables.find(level)->second.size() <= level_config.find(level)->second.first) return;

    //todo:add
//    cout << "compaction level: " << level << endl;

    uint64_t max_time_stamp = 0;
    bool isLastLevel = false;
    if(sstables.size() - 1 == level) isLastLevel = true;

    //读取数据
    auto *data = new map<uint64_t,pair<string,pair<uint64_t,uint64_t>>>(); //key,<value,<time_stamp,tag>>
    //LEVEL X
    if(level_config.find(level)->second.second == Tiering){
        string dir_name = this->dir + "/level-" + to_string(level);
        vector<string> file_list;
        utils::scanDir(dir_name,file_list);
        for(auto & file_name:file_list){
            string file_path = dir_name + "/" + file_name;
            read_data(file_path,data,isLastLevel);
            delete_sstables_file(file_path);
            if(max_time_stamp < get_time_stamp(file_path)){
                max_time_stamp = get_time_stamp(file_path);
            }
        }
    }else if(level_config.find(level)->second.second == Leveling){
        string dir_name = this->dir + "/level-" + to_string(level);
        vector<string> file_list;
        utils::scanDir(dir_name,file_list);
        vector<string> file_path_list;
        for(auto &file:file_list){
            file_path_list.push_back(dir_name + "/" + file);
        }
        vector<string> select_file_path;
        select_file(file_path_list, select_file_path, file_list.size() - level_config.find(level)->second.first);
//        for(int i = 0; i < file_list.size() - level_config.find(level)->second.first; ++i){
//            string file_path = dir_name + "/" + file_list[i];
//            read_data(file_path,data,isLastLevel);
//            delete_sstables_file(file_path);
//            if(max_time_stamp < get_time_stamp(file_path)){
//                max_time_stamp = get_time_stamp(file_path);
//            }
//        }
        for(auto &file_path:select_file_path){
            read_data(file_path,data,isLastLevel);
            delete_sstables_file(file_path);
            if(max_time_stamp < get_time_stamp(file_path)){
                max_time_stamp = get_time_stamp(file_path);
            }
        }
    }

    //LEVEL X+1
    //有下一层
    if(sstables.size() > level+1){
        if(level_config.find(level)->second.second == Leveling){
            uint64_t min_key = data->begin()->first;
            uint64_t max_key = data->end()->first;
            vector<string> file_path_to_delete;
            for(auto &time_tag_sstable : sstables.find(level+1)->second){
                SStable *tmp = time_tag_sstable.second;
                if(tmp->get_max_key() >= min_key && tmp->get_min_key() <= max_key){
                    string file_path = this->dir + "/level-" + to_string(level+1) + "/" + to_string(time_tag_sstable.first.first) + "_" + to_string(time_tag_sstable.first.second) + ".ssh";
                    read_data(file_path,data,isLastLevel);
//                    delete_sstables_file(file_path);
                    file_path_to_delete.push_back(file_path);
                    if(max_time_stamp < get_time_stamp(file_path)){
                        max_time_stamp = get_time_stamp(file_path);
                    }
                }
            }
            //删除文件
            for(auto file_path : file_path_to_delete){
                delete_sstables_file(file_path);
            }
        }
    }

    //生成新文件
    auto *tmp_mem_table = new SkipList<uint64_t,string>();
    auto iter = data->begin();
    while(iter != data->end()){
        if(tmp_mem_table->getMemSize() + 12 + iter->second.first.size() <= MEMTABLE_MAXSIZE){
            tmp_mem_table->insert(iter->first,iter->second.first);
            ++iter;
            continue;
        }
        //mem_table size > 2MB
        string dir_path = this->dir + "/level-" + to_string(level+1);
        SStable *sstable_new = SStable::memtable_to_sstable(dir_path,tmp_mem_table,max_time_stamp,tag);
        if(sstables.size() <= level + 1){
            sstables.insert({level+1,{}});
        }
        sstables.find(level+1)->second.insert(make_pair(make_pair(max_time_stamp,tag),sstable_new));
        ++tag;
        tmp_mem_table->clear();
        tmp_mem_table->insert(iter->first,iter->second.first);
        ++iter;
    }

    if(tmp_mem_table->getItemNum() != 0){
        string dir_path = this->dir + "/level-" + to_string(level+1);
        SStable *sstable_new = SStable::memtable_to_sstable(dir_path,tmp_mem_table,max_time_stamp,tag);
        if(sstables.size() <= level + 1){
            sstables.insert({level+1,{}});
        }
        sstables.find(level+1)->second.insert(make_pair(make_pair(max_time_stamp,tag),sstable_new));
        ++tag;
        tmp_mem_table->clear();
    }

    //todo:add
//    cout << "Finish compaction level: " << level <<endl;
    compaction(level+1);
}

void KVStore::read_from_disk() {
    uint64_t max_time_stamp = 0;
    vector<string> dir_list;
    utils::scanDir(dir,dir_list);
    for(auto &dir_name : dir_list){
        if(dir_name == "." || dir_name == "..") continue;
        string dir_path = dir + "/" + dir_name;
        vector<string> file_list;
        utils::scanDir(dir_path, file_list);
        for(auto &file_name : file_list){
            if(file_name == "." || file_name == "..") continue;
            string file_path = dir_path + "/" + file_name;
            //file_path:this->dir + "/level-0/1_1.ssh"
            //file_name:"1_1.ssh"
            //dir_name:"level-0"
            string tmp;
            tmp = file_name;
            tmp = tmp.substr(0, tmp.size() - 4);
            tmp = tmp.substr(tmp.find_last_of("_") + 1);
            uint64_t tag_cur = stoull(tmp);

            tmp = file_name;
            tmp = tmp.substr(0,tmp.find_first_of('_'));
            uint64_t time_stamp_cur = stoull(tmp);

            tmp = dir_name;
            tmp = tmp.substr(tmp.find_last_of("-") + 1);
            uint64_t level_cur = stoull(tmp);

            if(time_stamp_cur > max_time_stamp){
                max_time_stamp = time_stamp_cur;
            }

            SStable *sstable = SStable::read_sstable_from_disk(file_path);
            if(sstables.size() <= level_cur){
                sstables.insert({level_cur,{}});
            }
            sstables.find(level_cur)->second.insert(make_pair(make_pair(time_stamp_cur,tag_cur),sstable));

//            cout << "read_from_disk: " << file_path << endl;
        }
    }
    this->time_stamp = max_time_stamp + 1;
}

void KVStore::read_config() {
    ifstream file;
    file.open("../default.conf", ios::in);
    if(!file.is_open()){
        cout << "open file error" << endl;
        return;
    }

    string line;
    while(getline(file,line)){
        stringstream ss(line);
        uint64_t level;
        uint64_t max_num;
        mode mode;
        string tmp;
        ss >> level >> max_num >> tmp;
        if(tmp == "Tiering"){
            mode = Tiering;
        }else if(tmp == "Leveling"){
            mode = Leveling;
        }
        auto iter = level_config.find(level);
        if(iter == level_config.end()){
            level_config.insert({level,{}});
        }
        this->level_config.find(level)->second = make_pair(max_num, mode);
    }
    cout << "Finish config" << endl;
}

void KVStore::read_data(const string& file_path, map<uint64_t, pair<string,pair<uint64_t,uint64_t>>> *data, bool isLast) {
    //todo:add this
//    cout << "Read from :"<< file_path<<endl;
    //file_path:this->dir + "/level-0/1_1.ssh"
    string tmp = file_path;
    tmp = tmp.substr(tmp.find_first_of('-') + 1,tmp.find_last_of('/') - tmp.find_first_of('-') - 1);
    uint64_t level = stoull(tmp);

    tmp = file_path;
    tmp = tmp.substr(tmp.find_last_of('/') + 1, tmp.find_last_of('_') - tmp.find_last_of('/') - 1);
    uint64_t time_stamp_cur = stoull(tmp);

    tmp = file_path;
    tmp = tmp.substr(0, tmp.size() - 4);
    tmp = tmp.substr(tmp.find_last_of('_') + 1);
    uint64_t tag_cur = stoull(tmp);

    SStable *sstable = this->sstables.find(level)->second.find(make_pair(time_stamp_cur,tag_cur))->second;

    ifstream file;
    file.open(file_path,ios::in);
    if(!file.is_open()){
        cout << "File can't open";
        return;
    }

    for(int i = 0; i < sstable->get_pair_num() - 1; ++i){
        file.seekg(sstable->index_area[i].getOffset());
        uint32_t size = sstable->index_area[i+1].getOffset() - sstable->index_area[i].getOffset();
        char *buf = new char [size+1];
        buf[size] = '\0';
        file.read(buf,size);
        //make_pair用的是值传递还是引用传递，是否会产生函数结束buf析构但申请的空间还在
        //值传递
        uint64_t key = sstable->index_area[i].getKey();
        string value(buf);
        delete [] buf;

//        //删掉前一个
//        if (data && !data->empty()) {
//            auto iter = data->find(key);
//            if (iter != data->end()) {
//                data->erase(iter);
//            }
//        }
        if(!data->empty()){
            auto iter = data->find(key);
            if(iter != data->end()){
                if((time_stamp_cur > iter->second.second.first) || (time_stamp_cur == iter->second.second.first && tag_cur > iter->second.second.second)){
                    data->erase(key);
                }
            }
        }
        //如果是最后一层并且是"~DELETED~"，就不插入
        if(!(isLast && value == "~DELETED~")){
            data->insert(make_pair(key, make_pair(value, make_pair(time_stamp_cur,tag_cur))));
        }

    }
    //last element
    file.seekg(0, ios::end);
    streampos pos = file.tellg();
    uint32_t file_size = pos;
    uint32_t size = file_size - sstable->index_area[sstable->get_pair_num()-1].getOffset();
    char *buf = new char [size+1];
    buf[size] = '\0';
    file.seekg(sstable->index_area[sstable->get_pair_num()-1].getOffset());
    file.read(buf,size);

    uint64_t key = sstable->index_area[sstable->get_pair_num()-1].getKey();
    string value(buf);
    delete [] buf;

    if(!data->empty()){
        auto iter = data->find(key);
        if(iter != data->end()){
            if((time_stamp_cur > iter->second.second.first) || (time_stamp_cur == iter->second.second.first && tag_cur > iter->second.second.second)){
                data->erase(key);
            }
        }
    }
    //如果是最后一层并且是"~DELETED~"，就不插入
    if(!(isLast && value == "~DELETED~")){
        data->insert(make_pair(key, make_pair(value, make_pair(time_stamp_cur,tag_cur))));
    }

     file.close();
}

void KVStore::delete_sstables_file(const string &file_path) {
    string tmp = file_path;
    tmp = tmp.substr(tmp.find_first_of('-') + 1,tmp.find_last_of('/') - tmp.find_first_of('-') - 1);
    uint64_t level = stoull(tmp);

    tmp = file_path;
    tmp = tmp.substr(tmp.find_last_of('/') + 1, tmp.find_last_of('_') - tmp.find_last_of('/') - 1);
    uint64_t time_stamp = stoull(tmp);

    tmp = file_path;
    tmp = tmp.substr(0, tmp.size() - 4);
    tmp = tmp.substr(tmp.find_last_of('_') + 1);
    uint64_t tag = stoull(tmp);

    sstables.find(level)->second.erase(make_pair(time_stamp,tag));
    utils::rmfile(file_path.c_str());
}

uint64_t KVStore::get_time_stamp(const string &file_path) {
    string tmp = file_path;
    tmp = tmp.substr(tmp.find_last_of('/') + 1, tmp.find_last_of('_') - tmp.find_last_of('/') - 1);
    uint64_t time_stamp = stoull(tmp);
    return time_stamp;
}

uint64_t KVStore::get_min_key(const string& file_path)
{
    //file_path:"./data/level-0/1_1.ssh"
    string tmp = file_path;
    tmp = tmp.substr(tmp.find_first_of('-') + 1,tmp.find_last_of('/') - tmp.find_first_of('-') - 1);
    uint64_t level = stoull(tmp);

    tmp = file_path;
    tmp = tmp.substr(tmp.find_last_of('/') + 1, tmp.find_last_of('_') - tmp.find_last_of('/') - 1);
    uint64_t time_stamp_cur = stoull(tmp);

    tmp = file_path;
    tmp = tmp.substr(0, tmp.size() - 4);
    tmp = tmp.substr(tmp.find_last_of('_') + 1);
    uint64_t tag_cur = stoull(tmp);

    return sstables.find(level)->second.find(make_pair(time_stamp_cur,tag_cur))->second->get_min_key();
}

bool KVStore::compareFilePaths(const std::string& filePath1, const std::string& filePath2)
{
    uint64_t timeStamp1 = get_time_stamp(filePath1);
    uint64_t timeStamp2 = get_time_stamp(filePath2);

    if (timeStamp1 == timeStamp2)
    {
        uint64_t minKey1 = get_min_key(filePath1);
        uint64_t minKey2 = get_min_key(filePath2);

        return minKey1 < minKey2;
    }

    return timeStamp1 < timeStamp2;
}

void KVStore::select_file(vector<string> file_path_list, vector<string> &file_select_list, uint64_t num)
{
    std::sort(file_path_list.begin(), file_path_list.end(), [this](const std::string& filePath1, const std::string& filePath2)
    {
        return compareFilePaths(filePath1, filePath2);
    });
    file_select_list.assign(file_path_list.begin(), file_path_list.begin() + std::min(num, static_cast<uint64_t>(file_path_list.size())));

}

std::string KVStore::get_without_sstables(uint64_t key)
{
    string value = mem_table->serach(key);
    if(value == "~DELETED~") return "";
    if(value != "") return value;

    string value_possible = "";
    uint64_t time_stamp_max = 0;
    uint64_t tag_max = 0;
    vector<string> dir_list;
    utils::scanDir(dir,dir_list);
    for(auto &dir_name : dir_list) {
        if (dir_name == "." || dir_name == "..") continue;
        string dir_path = dir + "/" + dir_name;
        vector<string> file_list;
        utils::scanDir(dir_path, file_list);
        for (auto &file_name: file_list) {
            if (file_name == "." || file_name == "..") continue;
            string file_path = dir_path + "/" + file_name;
            SStable *new_sstable = SStable::read_sstable_from_disk(file_path);
            if(!(value = new_sstable->search(key,file_path)).empty()){
                string tmp = file_path;
                tmp = tmp.substr(0, tmp.size() - 4);
                tmp = tmp.substr(tmp.find_last_of('_') + 1);
                uint64_t tag = stoull(tmp);
                if((get_time_stamp(file_path) > time_stamp_max) || (get_time_stamp(file_path) == time_stamp_max && tag > tag_max)){
                    value_possible = value;
                    time_stamp_max = get_time_stamp(file_path);
                    tag_max = tag;
                }
            }
        }
    }

    if(value_possible == "~DELETED~") return "";
    if(value_possible != "") return value_possible;
    return "";
}

std::string KVStore::get_without_bloom(uint64_t key)
{
    string value = mem_table->serach(key);
    if(value == "~DELETED~") return "";
    if(value != "") return value;

    string value_possible = "";
    uint64_t time_stamp_max = 0;
    uint64_t tag_max = 0;
    //FINISH:handle the "~DELETED~" in sstable
    for(auto &sstable_level_time_tag : sstables){
        for(auto &sstable_time_tag: sstable_level_time_tag.second){
            SStable* sstable_cur = sstable_time_tag.second;
            const string file_path = dir + "/level-" + to_string(sstable_level_time_tag.first) + "/" + to_string(sstable_time_tag.first.first) + "_" + to_string(sstable_time_tag.first.second) + ".ssh";
            if(!(value = sstable_cur->serach_without_bloom(key,file_path)).empty()){
//                cout << "the value size return form KVStore::get is " << value.size() << endl; //FINISH:delete this line
//                if(value == "~DELETED~") return "";
//                return value;
                if((sstable_time_tag.first.first > time_stamp_max) || (sstable_time_tag.first.first == time_stamp_max && sstable_time_tag.first.second > tag_max)){
                    value_possible = value;
                    time_stamp_max = sstable_time_tag.first.first;
                    tag_max = sstable_time_tag.first.second;
                }
            }
        }
    }
    if(value_possible == "~DELETED~") return "";
    if(value_possible != "") return value_possible;
    return "";
}