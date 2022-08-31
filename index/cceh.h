#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <cstdio>
#include <queue>
#include <vector>
#include <iostream>

#include "pmallocator.h"
#include "flush.h"

// #define  MSB


namespace cceh {
    using std::string;
    using std::cout;
    using std::endl;

    const _key_t INVALID = -1;
    
    // const size_t kCacheLineSize = 64;
    constexpr size_t kSegmentBits = 8;
    constexpr size_t kBucketSize = CACHE_LINE_SIZE;
    constexpr size_t kMask = (1ull << kSegmentBits) - 1ull;
    constexpr size_t kSegmentSize = (1ull << kSegmentBits) * kBucketSize;
    constexpr size_t kNumBucket = 8;
    constexpr size_t kNumPairPerBucket = kBucketSize / 16;



    struct  segment
    {
        static const size_t kNumSlot = kSegmentSize / sizeof(Record);
        Record slot_[kNumSlot];
        uint64_t local_depth_;


        segment() {
            
        }

        void * operator new(size_t size) {
            return galc->malloc(size);
        }

        bool Get(_key_t key, _value_t& value) {
            uint64_t f_hash = hash1(key);
            uint64_t f_index = (f_hash & kMask) * kNumPairPerBucket;
            for (size_t i = 0; i < kNumBucket * kNumPairPerBucket; ++i) {
                auto loc = (f_index + i) % kNumSlot;
                auto k = slot_[loc].key;
                if (k == key) {
                    value = slot_[loc].val;
                    return true;
                }
            }

            uint64_t s_hash = hash2(key);
            uint64_t s_index = (s_hash & kMask) * kNumPairPerBucket;
            for (size_t i = 0; i < kNumBucket * kNumPairPerBucket; ++i) {
                auto loc = (s_index + i) % kNumSlot;
                auto k = slot_[loc].key;
                if (k == key) {
                    value = slot_[loc].val;
                    return true;
                }
            }

            return false;
        }

        bool Insert(_key_t key, _value_t value) {
            uint64_t f_hash = hash1(key);
            uint64_t f_index = (f_hash & kMask) * kNumPairPerBucket;
            for (size_t i = 0; i < kNumBucket * kNumPairPerBucket; ++i) {
                auto loc = (f_index + i) % kNumSlot;
                auto k = slot_[loc].key;
                if (k == INVALID) {
                    slot_[loc].val = value;
                    mfence();
                    slot_[loc].key = key;
                    clwb(&(slot_[loc]), sizeof(Record));
                    return true;
                }
            }

            uint64_t s_hash = hash2(key);
            uint64_t s_index = (s_hash & kMask) * kNumPairPerBucket;
            for (size_t i = 0; i < kNumBucket * kNumPairPerBucket; ++i) {
                auto loc = (s_index + i) % kNumSlot;
                auto k = slot_[loc].key;
                if (k == INVALID) {
                    slot_[loc].val = value;
                    mfence();
                    slot_[loc].key = key;
                    clwb(&(slot_[loc]), sizeof(Record));
                    return true;
                }
            }

            // split

        }

        void InsertWithoutClwb(_key_t key, _value_t value, char fp) {
            
        }

        uint64_t GetLocalDepth() {
            return local_depth_;
        }

    }__attribute__((aligned(PMLINE)));

}
