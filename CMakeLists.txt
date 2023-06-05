cmake_minimum_required(VERSION 3.22)
project(LSM_KV)

set(CMAKE_CXX_STANDARD 14)

add_executable(LSM_KV levelTest.cpp kvstore.h kvstore.cc kvstore_api.h skipList.h sstable.h bloomfilter.h bloomfilter.cpp sstable.h sstable.cpp diskstorage.cpp diskstorage.h)
