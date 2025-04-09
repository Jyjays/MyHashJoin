#include <chrono>
#include <iostream>
#include <random>

#include "gtest/gtest.h"
#include "hashjoin.h"  // 假设你的 HashTable 定义在 hashjoin.h 中

namespace hashjoin {


// 生成随机数据的辅助函数
std::vector<std::pair<int, int>> generate_random_data(size_t size,
                                                      int key_range,
                                                      int value_range) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> key_dist(1, key_range);      // 键的范围
  std::uniform_int_distribution<> value_dist(1, value_range);  // 值的范围

  std::vector<std::pair<int, int>> data;
  data.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    data.emplace_back(key_dist(gen), value_dist(gen));
  }
  return data;
}

// 设置随机数据参数
const size_t data_size = 1000000;  
const int key_range = 10000;        
const int value_range = 1000;      
auto R = generate_random_data(data_size, key_range, value_range);
auto S = generate_random_data(data_size, key_range, value_range);
const int num_threads = 8;  // 线程数
const size_t table_size = R.size() / 100 + 7;  // 哈希表大小


// 测试用例：测量 HashJoin 的运行时间
TEST(HashJoinTest, PerformanceTestWith100K) {
  // 创建 HashTable 对象
  HashTable ht(table_size);  // 默认构造函数，假设 buckets 大小已设置

  // 测量 Build 阶段时间
  auto build_start = std::chrono::high_resolution_clock::now();
  ht.Build(R);
  auto build_end = std::chrono::high_resolution_clock::now();
  auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      build_end - build_start);

  // 测量 Probe 阶段时间
  auto probe_start = std::chrono::high_resolution_clock::now();
  auto result = ht.Probe(S);
  auto probe_end = std::chrono::high_resolution_clock::now();
  auto probe_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      probe_end - probe_start);

  // 输出结果
  std::cout << "Build time: " << build_duration.count() << " ms\n";
  std::cout << "Probe time: " << probe_duration.count() << " ms\n";
  std::cout << "Total time: " << (build_duration + probe_duration).count()
            << " ms\n";
  std::cout << "Match count: " << result.size() << "\n";
}

TEST(HashJoinTest, MutiThreadTest) {

//   auto start = std::chrono::high_resolution_clock::now();
  auto res = multi_threaded_hash_join(R, S, num_threads, table_size);
//   auto end = std::chrono::high_resolution_clock::now();
//   auto duration =
//       std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
//   std::cout << "Multi-threaded HashJoin time: " << duration.count() << " ms\n";
//   std::cout << "Join result size: " << res.size() << "\n";
}

}  // namespace hashjoin

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}