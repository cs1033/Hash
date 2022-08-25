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

// #define MSB // LSB

namespace extendable {
    using std::string;
    using std::cout;
    using std::endl;
    
    const uint64_t ASSOC_NUM = 13;



    struct  bucket
    {
        state_t state_;
        char fingerprints_[ASSOC_NUM];
        char dumnt_;
        uint64_t local_depth_[2];
        Record slot_[ASSOC_NUM];
        bucket* next_[2];


        bucket() {
            state_.pack = 0;
        }

        void * operator new(size_t size) {
            return galc->malloc(size);
        }

        bool Get(_key_t key, _value_t& value) {
            char fp = finger_print(key);
            for (size_t i = 0; i < ASSOC_NUM; ++i) {
                if (state_.read(i) && fingerprints_[i] == fp && slot_[i].key == key) {
                    value = (_value_t)slot_[i].val;
                    return true;
                }
            }
            return false;
        }

        bool Insert(_key_t key, _value_t value) {
            char fp = finger_print(key);
            if (state_.count() == ASSOC_NUM) {
                return false;
            } else {
                auto slotid = state_.alloc();
                slot_[slotid] = {key, value};
                fingerprints_[slotid] = fp;
                clwb(&slot_[slotid], sizeof(Record));
                clwb(&fingerprints_[slotid], sizeof(Record));
                mfence();

                state_t new_state = state_;
                new_state.unpack.bitmap = state_.add(slotid);
                state_.pack = new_state.pack;
                clwb(&state_, 64);
                return true;
            }
        }

        void InsertWithoutClwb(_key_t key, _value_t value, char fp) {
            auto slotid = state_.alloc();
            slot_[slotid] = {key, value};
            fingerprints_[slotid] = fp;

            state_t new_state = state_;
            new_state.unpack.bitmap = state_.add(slotid);
            state_.pack = new_state.pack;
        }

        uint64_t GetLocalDepth() {
            return local_depth_[state_.unpack.version];
        }

        bucket* Split() {
            bucket* sibling = (bucket*) galc->malloc(sizeof(bucket));
            auto new_version = (state_.unpack.version + 1) % 2;
            auto new_state = state_;
            new_state.unpack.version = new_version;

            /* move entries */
            for (int i = 0; i < ASSOC_NUM; ++i) {
                uint64_t hash = hash1(slot_[i].key);
                uint64_t and_value = ((uint64_t)1ull << (local_depth_[state_.unpack.version] - 1ull));
            #ifndef LSB
                and_value = (uint64_t)1ull << (63 - local_depth_[state_.unpack.version]);
            #endif
                uint64_t index = hash & and_value; 

                if (index) {
                    new_state.unpack.bitmap = new_state.free(i);
                    sibling->InsertWithoutClwb(slot_[i].key, slot_[i].val, fingerprints_[i]);
                }
            }

            /* sibling ptr */
            sibling->next_[0] = next_[state_.unpack.version];
            next_[new_version] = galc->relative(sibling);

            /* local depth */
            local_depth_[new_version] = local_depth_[state_.unpack.version] + 1;
            sibling->local_depth_[0] = local_depth_[new_version];

            /* persist */
            clwb(sibling, sizeof(bucket));
            mfence();
            state_ = new_state;
            clwb(&state_, 8);

            return sibling;
        }

    }__attribute__((aligned(PMLINE)));

    struct directory
    {
        bucket** buckets_;         
    }__attribute__((aligned(CACHE_LINE_SIZE)));


    struct entrance
    {
        bucket*     far_left_bucket_;
        uint64_t    global_depth_;
    };

    class exHash {
    public:
        exHash(string path, bool recover) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "exHash");
            
                /* allocate*/
                entrance_ = (entrance *) galc->get_root(sizeof(entrance));
                dir_ = new directory();
                dir_->buckets_ = (bucket**)malloc(sizeof(bucket*) * 2);
                bucket* zero = (bucket*) galc->malloc(sizeof(bucket));
                bucket* one  = (bucket*) galc->malloc(sizeof(bucket));

                /* assign */
                zero->next_[0] = galc->relative(one);
                zero->local_depth_[0] = 1;
                one->next_[0]  = nullptr;
                one->local_depth_[0] = 1;
                entrance_->far_left_bucket_ = galc->relative(zero);
                entrance_->global_depth_ = 1;
                dir_->buckets_[0] = zero;
                dir_->buckets_[1] = one;

                /* persist  */
                clwb(zero, sizeof(bucket));
                clwb(one, sizeof(bucket));
                mfence();

                clwb(entrance_, sizeof(entrance));
            } else {
                
            }

            
        }

        ~exHash() {
            cout << "depth:" << entrance_->global_depth_ << endl;
        }

        bool Get(_key_t key, _value_t& value) {
            uint64_t f_hash = hash1(key);
            uint64_t and_value = ( ((uint64_t)1ull << entrance_->global_depth_) - (uint64_t)1ull);
        #ifndef LSB
            // and_value = and_value << (64 - entrance_->global_depth_);
            f_hash = f_hash >> (64 - entrance_->global_depth_);
        #endif
            uint64_t f_index = f_hash & and_value; 

            bucket* f_bucket = dir_->buckets_[f_index];
 
            // if (f_bucket->Get(key, value)) {
            //     return true;
            // } else {
            //     cout << "f_index:" << std::hex  << f_index << std::dec << endl;
            //     return false;
            // }

            return  f_bucket->Get(key, value);
        }

        bool Insert(_key_t key, _value_t value) {
        Expand:
            uint64_t f_hash = hash1(key);
            uint64_t and_value = ( ((uint64_t)1ull << entrance_->global_depth_) - (uint64_t)1ull);
        #ifndef LSB
            // and_value = and_value << (64 - entrance_->global_depth_);
            f_hash = f_hash >> (64 - entrance_->global_depth_);
        #endif
            uint64_t f_index = f_hash & and_value; 

        Split:
            bucket* f_bucket = dir_->buckets_[f_index];
            if (f_bucket->Insert(key, value) ) {
                return true;
            } else {
                bucket* _bucket = nullptr;
                uint64_t index;
                if (f_bucket->local_depth_[f_bucket->state_.unpack.version] < entrance_->global_depth_) {
                    _bucket = f_bucket;
                    index = f_index;
                } 
                if (_bucket != nullptr) {
                    auto local_depth = _bucket->GetLocalDepth();
                    bucket* sibling =  _bucket->Split();
                
                #ifndef LSB
                    uint64_t pre = index & ( ~(((uint64_t)1ull << (entrance_->global_depth_ - local_depth)) - 1ull));
                    uint64_t start = (uint64_t)1ull << (entrance_->global_depth_ - local_depth - 1ull);
                    uint64_t len = start;

                    for (uint64_t i = 0; i < len; ++i) {
                        uint64_t index = i + pre + start;
                        dir_->buckets_[index] = sibling;
                    }
                #else
                    //TODO:
                #endif

                    goto Split;
                } else {
                    DirExpand();
                    goto Expand;
                }
            }
        }

    private:
        void DirExpand() {
            entrance_->global_depth_++;
            uint64_t new_size = (uint64_t)1ull << entrance_->global_depth_;
            bucket** new_buckets = (bucket**)malloc(sizeof(bucket*) * new_size);

        #ifndef LSB
            for (uint64_t i = 0; i < new_size; ++i) {
                new_buckets[i] = dir_->buckets_[i/2];
            }
        #else
            //TODO
        #endif

            free(dir_->buckets_);
            dir_->buckets_ = new_buckets;

            clwb(&entrance_->global_depth_, 8);
        }

    private:
        entrance*   entrance_;
        directory*  dir_;
    };
    
}

