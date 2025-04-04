#include <mutex>
#include <thread>
#include <vector>

namespace hashjoin {

class HashTable {
 public:
  explicit HashTable(size_t num_buckets = 10007) : buckets(num_buckets) {}
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
  };
  std::vector<Bucket> buckets;
};

void build_thread(const std::vector<std::pair<int, int>>& R, int start, int end,
                  HashTable& ht);
void probe_thread(const std::vector<std::pair<int, int>>& S, int start, int end,
                  const HashTable& ht,
                  std::vector<std::pair<int, int>>& output);
auto multi_threaded_hash_join(const std::vector<std::pair<int, int>>& R,
                              const std::vector<std::pair<int, int>>& S,
                              int num_threads = 8)
    -> std::vector<std::pair<int, int>>;

};  // namespace hashjoin
