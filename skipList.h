//
// Created by 15006 on 2023/5/5.
//

#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H


#include <stdint.h>
#include <cstdlib>
#include <cstring>
#include <iostream>

template<typename K, typename V>
struct Node {
private:
    K key;
    V value;

public:
    Node<K, V> **next;
    int level;

    Node() = default;

    Node(const K k, const V v, const int l) : key(k), value(v), level(l) {
        //level + 1, because array index is from 0 - level
        next = new Node<K, V>*[level + 1];
        // Fill next array with 0(NULL)
        memset(next, 0, sizeof(Node<K, V> *)*(level + 1));
    }

    ~Node() {
        delete[] next;
    }

    K getKey() const{return key;}
    V getValue()const{return value;}
    void setValue(V v){value = v;}
};

template<typename K, typename V>
class SkipList {
private:
    int maxLevel = 0;
    int curLevel = 0;
    uint64_t memorySize;
    uint64_t itemNum;

public:
    Node<K, V>* head;

    int get_random_level();
    explicit SkipList(int maxLevel = 16);
    int insert(K key, V value);
    void remove(K key);
    V serach(K key);
    void clear();

    uint64_t getMemSize(){return memorySize;}
    uint64_t getMinKey(){return head->next[0]->getKey();}
    uint64_t getMaxKey(){
        Node<K,V> *cur = head;
        while(cur->next[0] != NULL){
            cur = cur->next[0];
        }
        return cur->getKey();
    }
    uint64_t getItemNum(){return itemNum;}
};

template<typename K, typename V>
int SkipList<K,V>::get_random_level() {
    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < maxLevel) ? k : maxLevel;
    return k;
}

template<typename K, typename V>
SkipList<K,V>::SkipList(int maxLevel) {
    this->maxLevel = maxLevel;
    this->curLevel = 0;
    K k;
    V v;
    this->head = new Node<K,V>(k,v ,maxLevel);
    memorySize = 10272;
    itemNum = 0;
}

template<typename K, typename V>
int SkipList<K,V>::insert(K key, V value) {
    //find the node whose key less than key, store in update
    Node<K,V> *current = this->head;
    Node<K,V> *update[this->maxLevel+1];
    memset(update,0,sizeof(Node<K,V>*)*(maxLevel+1));
    for(int i = this->curLevel; i >= 0; i--){
        while(current->next[i] != NULL && current->next[i]->getKey() < key){
            current = current->next[i];
        }
        update[i] = current;
    }

    current = current->next[0];

    // if current node have key equal to searched key, change it
    if (current != NULL && current->getKey() == key) {
        current->setValue(value);
        return 1;
    }

    //insert a new node
    if (current == NULL || current->getKey() != key ){
        int randomLevel = get_random_level();
        //update the update array & curLevel
        if(randomLevel > curLevel){
            for(int i = curLevel+1; i < randomLevel+1; ++i){
                update[i] = head;
            }
            curLevel = randomLevel;
        }

        Node<K,V>* insertNode = new Node<K,V>(key,value,randomLevel);
        for(int i = 0; i <= randomLevel; ++i){
            insertNode->next[i] = update[i]->next[i];
            update[i]->next[i] = insertNode;
        }

        //update memory size
        memorySize += 8 + 4 + sizeof(value);

        itemNum++;
    }
    return 0;
}

template<typename K, typename V>
void SkipList<K, V>::remove(K key) {
    Node<K,V> *current = this->head;
    Node<K,V> *update[maxLevel+1];
    memset(update,0,sizeof (Node<K,V>*)*(maxLevel+1));

    for(int i = curLevel; i >= 0; --i){
        while (current->next[i] !=NULL && current->next[i]->getKey() < key) {
            current = current->next[i];
        }
        update[i] = current;
    }

    current = current->next[0];
    if(current != NULL && current->getKey() == key){
        for(int i = 0; i < curLevel; ++i){
            if(update[i]->next[i] != current)
                break;
            update[i]->next[i] = current->next[i];
        }

        while (curLevel > 0 && head->next[curLevel] == 0){
            --curLevel;
        }

        itemNum--;
    }
}

template<typename K, typename V>
V SkipList<K,V>::serach(K key)
{
    Node<K,V> *current = head;

    for(int i = curLevel; i >= 0; --i){
       while(current->next[i] && current->next[i]->getKey() < key){
           current = current->next[i];
       }
    }

    current = current->next[0];

    if(current && current->getKey() == key){
        return current->getValue();
    }

    return "";
}

template<typename K,typename V>
void SkipList<K,V>::clear() {
    Node<K,V> *cur = head;
    while(cur->next[0] != nullptr){
        Node<K,V> *tmp = cur;
        cur = cur->next[0];
        delete tmp;
    }
    delete cur;

    K k;
    V v;
    this->head = new Node<K,V>(k,v ,maxLevel);

    curLevel = 0;
    memorySize = 10272;
    itemNum = 0;
}

#endif //LSM_KV_SKIPLIST_H
