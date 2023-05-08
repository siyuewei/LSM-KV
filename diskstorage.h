//
// Created by 15006 on 2023/5/6.
//

#ifndef LSM_KV_DISKSTORAGE_H
#define LSM_KV_DISKSTORAGE_H

#include "sstable.h"
#include "skipList.h"
#include "utils.h"
#include <iostream>
#include <fstream>

using namespace std;

class DiskStorage {
public:
    /*
     * create sstable*(expect data_area) based on memtable
     * write sstable(include data_area) to disk
     */
};


#endif //LSM_KV_DISKSTORAGE_H
