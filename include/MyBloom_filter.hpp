#include <vector>
#include <functional>

class BloomFilter {
private:
    std::vector<bool> bits;
    size_t num_hashes;
    size_t size;

    size_t hash(int key, size_t seed) const {
        size_t h = std::hash<int>{}(key);
        h ^= seed;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h % size;
    }

public:
    BloomFilter(size_t size, size_t num_hashes) : bits(size, false), num_hashes(num_hashes), size(size) {}

    void insert(int key) {
        for (size_t i = 0; i < num_hashes; ++i) {
            bits[hash(key, i)] = true;
        }
    }

    bool contains(int key) const {
        for (size_t i = 0; i < num_hashes; ++i) {
            if (!bits[hash(key, i)]) {
                return false;
            }
        }
        return true;
    }
};
