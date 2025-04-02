#include <vector>
#include <thread>
#include <mutex>

namespace hashjoin
{

    class HashTable
    {
    public:
        auto Insert() -> bool ;
        auto Get() -> int;
    private:
        struct Bucket
        {
            std::mutex mtx;
            std::vector<std::pair<int, std::vector<int>>> entries;
        };
        std::vector<Bucket> buckets;
    };
};
