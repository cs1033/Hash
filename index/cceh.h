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
    constexpr size_t kSegmentBits = 5;
    constexpr size_t kBucketSize = CACHE_LINE_SIZE;
    constexpr size_t kMask = (1ull << kSegmentBits) - 1ull;
    constexpr size_t kSegmentSize = (1ull << kSegmentBits) * kBucketSize;
    constexpr size_t kNumBucket = 8;
    constexpr size_t kNumPairPerBucket = kBucketSize / 16;

    struct  segment;
    struct  directory; 
    struct entrance;

    
    


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
            uint64_t pattern = f_hash >> (64 - local_depth_);
            for (size_t i = 0; i < kNumBucket * kNumPairPerBucket; ++i) {
                auto loc = (f_index + i) % kNumSlot;
                auto k = slot_[loc].key;
                if (k == INVALID || (hash1(k) >> (64 - local_depth_)) != pattern) {
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
                if (k == INVALID || (hash1(k) >> (64 - local_depth_)) != pattern) {
                    slot_[loc].val = value;
                    mfence();
                    slot_[loc].key = key;
                    clwb(&(slot_[loc]), sizeof(Record));
                    return true;
                }
            }

            return false;
        }

        bool Insert4split(_key_t key, _value_t value) {
            uint64_t f_hash = hash1(key);
            uint64_t f_index = (f_hash & kMask) * kNumPairPerBucket;
            for (size_t i = 0; i < kNumBucket * kNumPairPerBucket; ++i) {
                auto loc = (f_index + i) % kNumSlot;
                auto k = slot_[loc].key;
                if (k == INVALID) {
                    slot_[loc].val = value;
                    slot_[loc].key = key;
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
                    slot_[loc].key = key;
                    return true;
                }
            }

            return false;
        }

        uint64_t GetLocalDepth() {
            return local_depth_;
        }

        segment* Split() {
            // cout << "1" << endl;
            segment* split_segment = (segment*)galc->malloc(sizeof(segment));
            for (size_t i = 0; i < segment::kNumSlot; i++) {
                split_segment->slot_[i].key = INVALID;
            }
            split_segment->local_depth_ = local_depth_ + 1;

            uint64_t pattern = 1ull << (64 - local_depth_ - 1);
            for (size_t i = 0; i < segment::kNumSlot; i++) { 
                auto k = slot_[i].key;
                if (hash1(k) & pattern) {
                    split_segment->Insert4split(k, slot_[i].val);
                }
            }
            
            clwb(split_segment, sizeof(segment));
            // cout << "2" << endl;
            return split_segment;
        }

    }__attribute__((aligned(PMLINE)));

    
    struct directory
    {
        segment** segments_;
        uint64_t    global_depth_;      
    }__attribute__((aligned(PMLINE)));

    struct entrance
    {
        directory* dir_;
    };
    
    

    class cceh {
    public:
        cceh(string path, bool recover) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "cceh");
                entrance_ = (entrance*)galc->get_root(sizeof(entrance));
                dir_ = (directory*) galc->malloc(sizeof(directory));
                segment* zero = (segment*)galc->malloc(sizeof(segment));
                segment* one = (segment*)galc->malloc(sizeof(segment));

                /* assign */
                for (size_t i = 0; i < segment::kNumSlot; i++) {
                    zero->slot_[i].key = INVALID;
                    one->slot_[i].key = INVALID;
                }
                zero->local_depth_ = 1;
                one->local_depth_ = 1;
                
                dir_->global_depth_ = 1ull;
                uint64_t capacity = 2ull;
                dir_->segments_ = (segment**)galc->malloc(sizeof(segment*) * capacity);
                dir_->segments_[0] = galc->relative(zero);
                dir_->segments_[1] = galc->relative(one);

                entrance_->dir_ = galc->relative(dir_);

                /*persist*/
                clwb(zero, sizeof(segment));
                clwb(one, sizeof(segment));
                mfence();
                clwb(dir_, sizeof(directory));
                mfence();
                clwb(entrance_, sizeof(entrance));

                cout << "init success!" << endl;
            }
        }
        ~cceh() {}

        bool Get(_key_t key, _value_t& value) {
            uint64_t f_hash = hash1(key);
            uint64_t f_index = f_hash >> ((sizeof(f_hash) * 8) - dir_->global_depth_);
            segment* f_segment = galc->absolute(dir_->segments_[f_index]) ;
            if (f_segment->Get(key, value)) {
                return true;
            }

            return false;
        }


        bool Insert(_key_t key, _value_t value) { 
        Redo:
            uint64_t f_hash = hash1(key);
            uint64_t f_index = f_hash >> ((sizeof(f_hash) * 8) - dir_->global_depth_);
            segment* f_segment = galc->absolute(dir_->segments_[f_index]) ;
            if (f_segment->Insert(key, value)) {
                return true;
            }
            
       
            /*split*/
            if (f_segment->local_depth_ >= dir_->global_depth_) {
                /* need to double the directory */
                DirExpand();
                goto Redo;
            } else { // normal segment split
                segment* split_segment = f_segment->Split();

                /* add split segment to directory */
                uint64_t pre = f_index & ( ~(((uint64_t)1ull << (dir_->global_depth_ - f_segment->local_depth_)) - 1ull));
                uint64_t start = (uint64_t)1ull << (dir_->global_depth_ - f_segment->local_depth_ - 1ull);
                uint64_t len = start;
                for (uint64_t i = 0; i < len; ++i) {
                    uint64_t pos = i + pre + start;
                    dir_->segments_[pos] = galc->relative(split_segment);
                }
                clwb(&(dir_->segments_[pre + start]), sizeof(segment*) * len);
                mfence();


                /*add origin segment's local depth*/
                f_segment->local_depth_++;
                clwb(&(f_segment->local_depth_), 8);
                goto Redo;
            }

        }

    private:
        void DirExpand() {

            directory* new_dir = (directory*)galc->malloc(sizeof(directory));
            new_dir->global_depth_ = dir_->global_depth_ + 1;
            auto new_capacity = 1ull << new_dir->global_depth_;
            new_dir->segments_ = (segment**)galc->malloc(sizeof(segment*) * new_capacity);

            for (size_t i = 0; i < new_capacity; ++i) {
                new_dir->segments_[i] = dir_->segments_[i/2];
            }

            entrance_->dir_ = galc->relative(new_dir);
            /*free old direcoty*/
            for (size_t i = 0; i < new_capacity/2; ++i) {
                galc->free(galc->absolute(dir_->segments_[i]));
            }
            galc->free(dir_);
            dir_ = new_dir;
        }


    private:
        directory* dir_;
        entrance* entrance_;
    };


}
