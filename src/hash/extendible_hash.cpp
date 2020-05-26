#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
: numBuckets_(0),globalDepth_(0),bucketSz_(size)
{
  bucketLists_.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const {
  return std::hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return globalDepth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return bucketLists_[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return numBuckets_;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  size_t hash_key = HashKey(key) & ((1 << globalDepth_) - 1);
  std::shared_ptr<Bucket> ptr = bucketLists_[hash_key];
  if (ptr->items.find(key) != ptr->items.end()) {
    value = ptr->items[key];
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  size_t hash_key = HashKey(key) & ((1 << globalDepth_)-1);
  std::shared_ptr<Bucket> ptr = bucketLists_[hash_key];
  if (ptr->items.find(key) != ptr->items.end()) {
    ptr->items.erase(key);
    return true;
  }
  return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  size_t hash_key = HashKey(key) & ((1 << globalDepth_) - 1);
  std::shared_ptr<Bucket> ptr = bucketLists_[hash_key];
  while (true) {
    // 存在key 或者 size小于bucketSz， 直接写入
    if (ptr->items.find(key) != ptr->items.end() || ptr->items.size() < bucketSz_) {
      ptr->items[key] = value;
      break;
    }

    // 扩容
    // step 1: 扩充localDepth， 如果大于globalDepth -> step 2, 小于 -> step 3
    // step 2: 扩充bucket list
    // step 3: 新增一个bucket，新的bucket，对原来旧的进行遍历，并和mask求交集，所有的为1的到新的bucket里面
    // step 4: 新bucket的位置，根据mask和i的并决定
    // step 5: while 迭代，一次可能不会完成
    int mask = (1 << (ptr->localDepth));
    ptr->localDepth++;
    if (ptr->localDepth > globalDepth_) {
      size_t length = bucketLists_.size();
      for (size_t i = 0; i < length; i++) {
        bucketLists_.push_back(bucketLists_[i]);
      }
      globalDepth_++;
    }
    numBuckets_++;
    auto newBuc = std::make_shared<Bucket>(ptr->localDepth);
    for (auto iter = ptr->items.begin(); iter != ptr->items.end(); ){
      if (HashKey(iter->first) & mask) {
        newBuc->items[iter->first] = iter->second;
        iter = ptr->items.erase(iter);
      } else iter++;
    }

    for (size_t i = 0; i < bucketLists_.size(); i++) {
      if (bucketLists_[i] == ptr && (i&mask)) {
        bucketLists_[i] = newBuc;
      }
    }
    hash_key = HashKey(key) & ((1 << globalDepth_) - 1);
    ptr = bucketLists_[hash_key];
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
