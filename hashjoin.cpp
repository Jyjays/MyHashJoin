#include "hashjoin.h"

namespace hashjoin {

//-----------public--------------
auto HashTable::Insert(int key, int value) -> void {
  auto& bucket = buckets[hash(key)];
  // Lock the bucket.
  std::lock_guard<std::mutex> lock(bucket.mtx);
  for (auto& entry : bucket.entries) {
    if (entry.first == key) {
      entry.second.push_back(value);
      return;
    }
  }
  bucket.entries.push_back({key, {value}});
}
auto HashTable::Get(int key) -> std::vector<int> {
  auto& bucket = buckets[hash(key)]; 
  for (auto& entry : bucket.entries) {
    if (entry.first == key) {
        return entry.second;
    }
  }
  return std::vector<int>();
}

//-----------build---------------

auto HashTable::Build(std::vector<std::pair<int,int>>& kvs) -> void {
    for(auto &kv : kvs) {
        Insert(kv.first, kv.second);
    }
}

//-----------probe---------------

auto HashTable::Probe(std::vector<std::pair<int,int>>& kvs) -> size_t {
    size_t count = 0;
    for (auto &kv : kvs) {
        auto res = Get(kv.first);
        for (int value : res) {
            count ++;
        }
    }
    return count;
}

//-----------utils---------------
auto HashTable::hash(int key) -> size_t {
  size_t hash_value = std::hash<int>{}(key);
  return hash_value % buckets.size();
}

auto HashTable::getCollisionCount(int key) -> size_t {
  auto& bucket = buckets[hash(key)];
  return bucket.entries.size();
}

}  // namespace hashjoin