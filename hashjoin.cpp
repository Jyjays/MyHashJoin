#include "hashjoin.h"

namespace hashjoin {

//-----------public--------------
void HashTable::Insert(int key, int value) {
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
auto HashTable::Get(int key) const -> std::vector<int> {
  auto& bucket = buckets[hash(key)];
  for (auto& entry : bucket.entries) {
    if (entry.first == key) {
      return entry.second;
    }
  }
  return std::vector<int>();
}

//-----------build---------------

void HashTable::Build(std::vector<std::pair<int, int>>& kvs) {
  for (auto& kv : kvs) {
    Insert(kv.first, kv.second);
  }
}

//-----------probe---------------

auto HashTable::Probe(std::vector<std::pair<int, int>>& kvs) -> std::vector<std::pair<int,int>> {
  std::vector<std::pair<int, int>> result;
  for (auto& kv : kvs) {
    int key = kv.first;
    int value_s = kv.second;
    auto values_r = Get(key);
    for (int value_r : values_r) {
      result.push_back({value_r, value_s});
    }
  }
  return result;
}

//-----------utils---------------
auto HashTable::hash(int key) const -> size_t {
  size_t hash_value = std::hash<int>{}(key);
  return hash_value % buckets.size();
}

auto HashTable::getCollisionCount(int key) -> size_t {
  auto& bucket = buckets[hash(key)];
  return bucket.entries.size();
}

//---------muti-thread---------------
void build_thread(const std::vector<std::pair<int, int>>& R, int start, int end,
                  HashTable& ht) {
  for (int i = start; i < end; ++i) {
    int key = R[i].first;
    int value = R[i].second;
    ht.Insert(key, value);
  }
}

void probe_thread(const std::vector<std::pair<int, int>>& S, int start, int end,
                  const HashTable& ht,
                  std::vector<std::pair<int, int>>& output) {
  for (int i = start; i < end; ++i) {
    int key = S[i].first;
    int value_s = S[i].second;
    auto values_r = ht.Get(key);
    for (int value_r : values_r) {
      output.push_back({value_r, value_s});
    }
  }
}

auto multi_threaded_hash_join(const std::vector<std::pair<int, int>>& R,
                              const std::vector<std::pair<int, int>>& S,
                              int num_threads)
    -> std::vector<std::pair<int, int>> {
  HashTable ht(R.size() / 10 + 7); 
  std::vector<std::thread> threads;

  // 构建阶段
  int N = R.size();
  for (int i = 0; i < num_threads; ++i) {
    int start = i * N / num_threads;
    int end = (i + 1) * N / num_threads;
    threads.emplace_back(build_thread, std::ref(R), start, end, std::ref(ht));
  }
  for (auto& t : threads) {
    t.join();
  }

  // 探测阶段
  threads.clear();
  int M = S.size();
  std::vector<std::vector<std::pair<int, int>>> outputs(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    int start = i * M / num_threads;
    int end = (i + 1) * M / num_threads;
    threads.emplace_back(probe_thread, std::ref(S), start, end, std::ref(ht),
                         std::ref(outputs[i]));
  }
  for (auto& t : threads) {
    t.join();
  }

  // 合并输出
  std::vector<std::pair<int, int>> final_output;
  for (auto& out : outputs) {
    final_output.insert(final_output.end(), out.begin(), out.end());
  }
  return final_output;
}

}  // namespace hashjoin