#include "hashjoin.h"

namespace hashjoin {

//-----------public--------------
void HashTable::Insert(int key, int value) {
#ifdef BLOOM_FILTER_ENABLE
  std::lock_guard<std::mutex> blm_lock(blm_mtx);
  blm_.insert(key);
#endif
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
#ifdef BLOOM_FILTER_ENABLE
  if (!blm_.contains(key)) {
    return std::vector<int>();
  }
#endif
  auto& bucket = buckets[hash(key)];
  for (const auto& entry : bucket.entries) {
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

auto HashTable::Probe(std::vector<std::pair<int, int>>& kvs)
    -> std::vector<std::pair<int, int>> {
  std::vector<std::pair<int, int>> result;
  for (auto& kv : kvs) {
    int key = kv.first;
    auto values_r = Get(key);  // Get the values from R table
    for (int value_r : values_r) {
      result.push_back({value_r, kv.second});
    }
    // if (blm_.contains(key)) {
    //   auto values_r = Get(key);  // Get the values from R table
    //   for (int value_r : values_r) {
    //     result.push_back({value_r, kv.second});
    //   }
    // }
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
                              int num_threads, size_t table_size)
    -> std::vector<std::pair<int, int>> {
  HashTable ht(table_size);
  std::vector<std::thread> threads;

  // Build
  int N = R.size();
  int process_num = N / num_threads;

  for (int i = 0; i < num_threads; ++i) {
    int start = i * process_num;
    int end = (i + 1) * process_num;
    if (i == num_threads - 1) {
      end = N;  // 最后一个线程处理剩余的元素
    }
    threads.emplace_back(build_thread, std::ref(R), start, end, std::ref(ht));
  }
  for (auto& t : threads) {
    t.join();
  }

  // Probe
  threads.clear();
  int M = S.size();
  std::vector<std::vector<std::pair<int, int>>> outputs(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    int start = i * process_num;
    int end = (i + 1) * process_num;
    if (i == num_threads - 1) {
      end = M;
    }
    threads.emplace_back(probe_thread, std::ref(S), start, end, std::ref(ht),
                         std::ref(outputs[i]));
  }
  for (auto& t : threads) {
    t.join();
  }

  // Merge results
  std::vector<std::pair<int, int>> final_output;
  for (auto& out : outputs) {
    final_output.insert(final_output.end(), out.begin(), out.end());
  }
  return final_output;
}

}  // namespace hashjoin