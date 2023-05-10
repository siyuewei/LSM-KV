#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    this->dir = dir;
    mem_table = new SkipList<uint64_t,string>();
    sstables = {{0,{}}};
    time_stamp = 1;
    tag = 1;
    //TODO:在启动时，需检查现有的数据目录中各层 SSTable 文件，并在内存中构建相应的缓存

}

KVStore::~KVStore()
{
    //TODO:系统在正常关闭时（可以实现在析构函数里面），应将MemTable 中的所有数据以 SSTable 形式写回（类似于 MemTable 满了时的操作）
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    //FINISH : check if the sstable size will larger than 2MB
    //TODO: check if the put is a update
    if(mem_table->getMemSize() + 12 + s.size() <= MEMTABLE_MAXSIZE){
        mem_table->insert(key, s);
        return;
    }

    //size > 2MB, create sstable
    //TODO:NOW IN LEVEL 0
    const string dir_path = dir + "/level-0";
    SStable *sstable_new = SStable::memtable_to_sstable(dir_path,mem_table,time_stamp,tag);
    mem_table->clear();

    sstables[0].insert({{time_stamp,tag},sstable_new});
    ++time_stamp;
    ++tag;

    mem_table->insert(key,s);//Don't forget to insert the new key-value pair
//    compaction();
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

    //TODO:handle the "~DELETED~" in sstable
    for(auto &sstable_level_time_tag : sstables){
        for(auto &sstable_time_tag: sstable_level_time_tag.second){
            SStable* sstable_cur = sstable_time_tag.second;
            const string file_path = dir + "/level-" + to_string(sstable_level_time_tag.first) + "/" + to_string(sstable_time_tag.first.first) + "_" + to_string(sstable_time_tag.first.second) + ".ssh";
            if(!(value = sstable_cur->search(key,file_path)).empty()){
                cout << "the value size return form KVStore::get is " << value.size() << endl; //TODO:delete this line
                return value;
            }
        }
    }
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
    for(auto &sstable_level_time_tag : sstables){
        for(auto &sstable_time_tag: sstable_level_time_tag.second){
            SStable* sstable_cur = sstable_time_tag.second;
            const string file_path = dir + "level-" + to_string(sstable_level_time_tag.first) + "/" + to_string(sstable_time_tag.first.first) + "_" + to_string(sstable_time_tag.first.second);
            utils::rmfile(file_path.c_str());
        }
        const string dir_path = dir + "level-" + to_string(sstable_level_time_tag.first);
        utils::rmdir(dir_path.c_str());
    }
    utils::rmdir(dir.c_str());

    /*sstables*/
    sstables.clear();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}

void KVStore::compaction() {

}