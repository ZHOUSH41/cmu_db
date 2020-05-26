/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() : _sz(0) {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> lockGuard(_mtx);
  for (auto iter = _replacer.begin(); iter != _replacer.end(); iter++) {
    if (*iter == value) {
      _replacer.erase(iter);
      _replacer.push_back(value);
      return;
    }
  }
  _replacer.push_back(value);
  _sz++;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lockGuard(_mtx);
  if (_sz == 0)
    return false;
  value = _replacer.front();
  _replacer.pop_front();
  _sz--;
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lockGuard(_mtx);
  for (auto iter = _replacer.begin(); iter != _replacer.end();) {
    if (*iter == value) {
      _replacer.erase(iter);
      _sz--;
      return true;
    }
    iter++;
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> lockGuard(_mtx);
  return _sz;
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
