#include <mutex>
#include <thread>
#include <vector>
#include "bloom_filter.hpp"

#define BLOOM_FILTER_ENABLE 

namespace hashjoin {

class HashTable {
 public:
  explicit HashTable(size_t num_buckets = 10007) : buckets(num_buckets),blm_() {}
  void Insert(int key, int value);
  auto Get(int key) const -> std::vector<int>;
  auto Build(std::vector<std::pair<int, int>>& kvs) -> void;

  /**
   * @param kvs The keys and values need to join.
   * @return The size of matched count.
   */
  auto Probe(std::vector<std::pair<int, int>>& kvs) -> std::vector<std::pair<int,int>> ;

 private:
  auto hash(int key) const -> size_t;
  auto getCollisionCount(int key) -> size_t;
  struct Bucket {
    std::mutex mtx;
    std::vector<std::pair<int, std::vector<int>>> entries;
    // int key;
    // std::vector<int> values;
  };
  std::vector<Bucket> buckets;

  std::mutex blm_mtx; // The mutex of bloom_filter
  bloom_filter blm_;
};

void build_thread(const std::vector<std::pair<int, int>>& R, int start, int end,
                  HashTable& ht);
void probe_thread(const std::vector<std::pair<int, int>>& S, int start, int end,
                  const HashTable& ht,
                  std::vector<std::pair<int, int>>& output);
auto multi_threaded_hash_join(const std::vector<std::pair<int, int>>& R,
                              const std::vector<std::pair<int, int>>& S,
                              int num_threads = 8, size_t table_size = 10007)
    -> std::vector<std::pair<int, int>>;

};  // namespace hashjoin
