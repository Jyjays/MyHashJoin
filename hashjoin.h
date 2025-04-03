#include <vector>
#include <thread>
#include <mutex>

namespace hashjoin
{

    class HashTable
    {
    public:
        explicit HashTable(size_t num_buckets = 10007) : buckets(num_buckets) {}
        auto Insert(int key, int value) -> void ;
        auto Get(int key ) -> std::vector<int>;
        auto Build(std::vector<std::pair<int,int>>& kvs) -> void;
        
        /**
         * @param kvs The keys and values need to join.
         * @return The size of matched count.
         */
        auto Probe(std::vector<std::pair<int,int>>& kvs) -> size_t;
    private:

        auto hash(int key) -> size_t;
        auto getCollisionCount(int key) -> size_t;
        struct Bucket
        {
            std::mutex mtx;
            std::vector<std::pair<int, std::vector<int>>> entries;
        };
        std::vector<Bucket> buckets;
    };
};
