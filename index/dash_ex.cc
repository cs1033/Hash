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



namespace dashEX {
    using std::string;
    using std::cout;
    using std::endl;


    constexpr size_t    kSegmentBits = 6;
    constexpr size_t    kBucketSize = PMLINE;
    constexpr size_t    kMask = (1ull << kSegmentBits) - 1ull;
    // constexpr size_t kSegmentSize = (1ull << kSegmentBits) * kBucketSize;
    constexpr size_t    kNumBucket =  1 << kSegmentBits;
    constexpr size_t    kNumStashBucket = 2;
    constexpr size_t    kNumPairPerBucket = 14;
    constexpr size_t    kNumOverflowPairPerBucket = 4;
    constexpr uint8_t   overflowBitmapMask = (1 << kNumOverflowPairPerBucket) - 1;
    constexpr size_t    stashMask = 32 - __builtin_clz(kNumStashBucket - 1);
    constexpr size_t    allocMask = (1 << kNumPairPerBucket) - 1;
    constexpr size_t    countMask = (1 << 32 - 2 * kNumPairPerBucket) - 1;
    constexpr uint32_t  slotMask = (~(0u)) - ((1u << kNumPairPerBucket) - 1); 


// #define BUCKET_INDEX(hash) ((hash >> kFingerBits) & bucketMask)
#define GET_COUNT(var) ((var) & countMask)
#define GET_MEMBER(var) (((var) >> ((32 - 2 * kNumPairPerBucket))) & allocMask)
// #define GET_INVERSE_MEMBER(var) ((~((var) >> 4)) & allocMask)
#define GET_BITMAP(var) ((var) >> (32 - kNumPairPerBucket))
#define CHECK_BIT(var, pos) ((((var) & (1 << pos)) > 0) ? (1) : (0))


    struct  bucket;
    struct  segment;
    struct  directory; 
    struct  entrance;
    
    struct bucket
    {
        //metadata
        uint32_t    version_lock_;
        uint32_t    bitmap_;                // allocation bitmap + pointer bitmap + counter
        char        fingerprints_[kNumPairPerBucket + kNumOverflowPairPerBucket];

        uint8_t     overflowBitmap_;
        uint8_t     overflowIndex_;
        uint8_t     overflowMembership_;     /*overflowmembership indicates membership of the overflow
                                            fingerprint*/
        uint8_t     overflowCount_;
        uint8_t     unused_[2];

        //records
        Record      slot_[kNumPairPerBucket];

        /*                         functions                                 */
        void initailize() {
            bitmap_ = 0;
            overflowBitmap_ = 0;
            overflowMembership_ = 0;
            overflowCount_ = 0;
        }

        bool Get(_key_t key, _value_t &value, char fp, bool probe) {
            int mask = 0;
            if (!probe) {
                mask = GET_BITMAP(bitmap_) & (~GET_MEMBER(bitmap_));
            } else {
                mask = GET_BITMAP(bitmap_) & GET_MEMBER(bitmap_);
            }

            for (size_t i = 0; i != kNumPairPerBucket; ++i) {
                if (CHECK_BIT(mask, i) && fingerprints_[i] == fp && slot_[i].key == key) {
                    value = slot_[i].val;
                    return true;
                }
            }

            return false;
        }

        bool Insert(_key_t key, _value_t value, char fp, bool probe) {
            uint32_t mask = GET_BITMAP(bitmap_) | slotMask;
            auto slot = __builtin_ctz(~mask) - (32u - kNumPairPerBucket);

            if (slot == kNumPairPerBucket) {
                return false;
            }

            slot_[slot].key = key;
            slot_[slot].val = value;
            fingerprints_[slot] = fp;

            uint32_t new_bitmap = bitmap_ | (1 << (slot + 32 - kNumPairPerBucket));
            if (probe) {
                new_bitmap = new_bitmap | (1 << (slot + 32 - 2 * kNumPairPerBucket));
            }
            new_bitmap += 1;
            bitmap_ = new_bitmap;

            return true;
        }

        bool test_overflow() { return overflowCount_; }

    }__attribute__((aligned(PMLINE)));
    
    


    struct  segment
    {
        //
        bucket      normalBucket_[kNumBucket];
        bucket      stashBucket_[kNumStashBucket];

        segment*    next_;
        size_t      local_depth_;
        int         state_;


        //
        void initialize() {
            for (auto &b : normalBucket_) {
                b.initailize();
            }
            for (auto &b : stashBucket_) {
                b.initailize();
            }
            next_ = nullptr;
        }

        void setDepth(size_t depth) {
            local_depth_ = depth;
        }

        bool Get(_key_t key, _value_t &value) {
            uint64_t f_hash  = hash1(key);
            uint64_t f_index = f_hash & kMask;
            char fp = finger_print(key);

            bucket* target_bucket   = normalBucket_ + f_index;
            bucket* neighbor_bucket = normalBucket_ + (1 + f_index) % kNumBucket;

            if (target_bucket->Get(key, value, fp, false)) {
                return true;
            }
            if (neighbor_bucket->Get(key, value, fp, true)) {
                return true;
            }


            //search stash buckets
            bool search_stash = false;
            if (target_bucket->test_overflow()) {
                search_stash = true;
            } else {
                uint64_t mask = target_bucket->overflowBitmap_ & (~(target_bucket->overflowMembership_)) & overflowBitmapMask ;
                for (size_t i = 0; i != kNumOverflowPairPerBucket; ++i) {
                    if (mask & (1ull << i) && target_bucket->fingerprints_[kNumPairPerBucket + i] == fp) {
                        bucket* stash = stashBucket_ + (target_bucket->overflowIndex_ >> (2 * i) & stashMask);
                        if (stash->Get(key, value, fp, false)) {
                            return true;
                        }
                    }
                }
            }

            if (neighbor_bucket->test_overflow()) {
                search_stash = true;
            } else {
                uint64_t mask = neighbor_bucket->overflowBitmap_ & (neighbor_bucket->overflowMembership_) & overflowBitmapMask ;
                for (size_t i = 0; i != kNumOverflowPairPerBucket; ++i) {
                    if (mask & (1ull << i) && neighbor_bucket->fingerprints_[kNumPairPerBucket + i] == fp) {
                        bucket* stash = stashBucket_ + (neighbor_bucket->overflowIndex_ >> (2 * i) & stashMask);
                        if (stash->Get(key, value, fp, false)) {
                            return true;
                        }
                    }
                }
            }

            if (search_stash) {
                for (size_t i = 0; i != kNumStashBucket; ++i) {
                    bucket* stash = stashBucket_ + i;
                    if (stash->Get(key, value, fp, false)) {
                        return true;
                    }
                }
            }

            return false;
        }


        bool Insert(_key_t key, _value_t value) {
            uint64_t f_hash  = hash1(key);
            uint64_t f_index = f_hash & kMask;
            char fp = finger_print(key);

            bucket* target_bucket   = normalBucket_ + f_index;
            bucket* neighbor_bucket = normalBucket_ + (1 + f_index) % kNumBucket;

            //balanced insert
            if (GET_COUNT(target_bucket->bitmap_) < kNumPairPerBucket || GET_COUNT(neighbor_bucket->bitmap_) < kNumPairPerBucket) {
                if (GET_COUNT(target_bucket->bitmap_) <= GET_COUNT(neighbor_bucket->bitmap_)) {
                    target_bucket->Insert(key, value, fp, false);
                } else {
                    neighbor_bucket->Insert(key, value, fp, true);
                }
            }

            //displace
            bucket* next_bucket = normalBucket_ + (2 + f_index) % kNumBucket;
            if (try_movement(neighbor_bucket, next_bucket, true) && neighbor_bucket->Insert(key, value, fp, true)) {
                return true;
            }

            bucket* pre_bucket = normalBucket_ + (f_index + kNumBucket - 1) % kNumBucket;
            if (try_movement(target_bucket, pre_bucket, false) && target_bucket->Insert(key, value, fp, false)) {
                return true;
            }

            //stash
            size_t pos = f_index % kNumStashBucket;
            for (size_t i = 0; i != kNumStashBucket; ++i) {
                bucket* stash = stashBucket_ + (pos + i) % kNumStashBucket;
                if (stash->Insert(key, value, fp, false)) {
                    if (_mm_popcnt_u32(target_bucket->overflowBitmap_) < kNumOverflowPairPerBucket) {
                        
                    }
                }
            }

        }

        bool try_movement(bucket* from, bucket* to, bool probe) {
            if (GET_COUNT(to->bitmap_) >= kNumPairPerBucket) {
                return false;
            }
            
            if (probe) {
                uint32_t mask = GET_BITMAP(from->bitmap_) & (~GET_MEMBER(from->bitmap_));
                for (size_t i = 0; i != kNumPairPerBucket; ++i) {
                    if (CHECK_BIT(mask, i)) {
                        auto k = from->slot_[i].key;
                        auto v = from->slot_[i].val;
                        char fp = finger_print(k);
                        to->Insert(k, v, fp, true);
                        from->bitmap_ &=  ~(1 << (i + 32 - kNumPairPerBucket));
                        return true;
                    }
                }
            } else {
                uint32_t mask = GET_BITMAP(from->bitmap_) & GET_MEMBER(from->bitmap_);
                for (size_t i = 0; i != kNumPairPerBucket; ++i) {
                    if (CHECK_BIT(mask, i)) {
                        auto k = from->slot_[i].key;
                        auto v = from->slot_[i].val;
                        char fp = finger_print(k);
                        to->Insert(k, v, fp, false);
                        from->bitmap_ &=  ~(1 << (i + 32 - kNumPairPerBucket));
                        from->bitmap_ &= ~(1 << (i + 32 - 2 * kNumPairPerBucket));
                        return true;
                    }
                }
            }
            
            return false;
        }

    }__attribute__((aligned(PMLINE)));

    
    struct directory
    {
        segment**   segments_;
        uint64_t    global_depth_;     
        uint64_t    version_; 
    }__attribute__((aligned(PMLINE)));

    struct entrance
    {
        directory* dir_;
    };
    
    

    class dashEX {
    public:
        dashEX(string path, bool recover) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "cceh");
                entrance_ = (entrance*)galc->get_root(sizeof(entrance));
                dir_ = (directory*) galc->malloc(sizeof(directory));
                segment* zero = (segment*)galc->malloc(sizeof(segment));
                segment* one = (segment*)galc->malloc(sizeof(segment));

                /* initialize */
                zero->initialize();
                zero->setDepth(1);
                one->initialize();
                one->setDepth(1);
                
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

        ~dashEX() {}


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
            } else {

            }
        }

    private:
        void DirExpand() {

        }

    private:
        directory* dir_;
        entrance* entrance_;
    };


}
