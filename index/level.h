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

#define PMLINE 256
#define ASSOC_NUM 15

#define F_IDX(hash, capacity) (hash % (capacity / 2))
#define S_IDX(hash, capacity) ((hash % (capacity / 2)) + (capacity / 2))

namespace level {
    using std::string;
    using std::cout;
    using std::endl;

    union state_t { // an 2 bytes states type
        //1
        uint16_t pack;
        //2
        struct unpack_t {
            uint16_t bitmap         : 14;
            uint16_t sib_version    : 1;
            uint16_t node_version   : 1;
        } unpack;

        inline uint8_t count() {
            return (uint8_t)_mm_popcnt_u32(unpack.bitmap);
        }

        inline bool read(int8_t idx) {
            return (unpack.bitmap & ((uint16_t)0x8000 >> (idx + 2))) > 0;
        }

        inline int8_t alloc() {
            uint32_t tmp = ((uint64_t)0xFFFFC000 | unpack.bitmap);
            return __builtin_ia32_lzcnt_u32(~tmp) - 18;
        }

        inline uint16_t add(int8_t idx) {
            return unpack.bitmap + ((uint16_t)0x8000 >> (idx + 2));
        }

        inline uint16_t free(int8_t idx) {
            return unpack.bitmap - ((uint16_t)0x8000 >> (idx + 2));
        }

        inline uint8_t get_sibver() {
            return (uint8_t) unpack.sib_version;
        }
    };

    struct  bucket
    {
        state_t state_;
        // char fingerprints_[ASSOC_NUM];
        char dumnt_[14];
        Record slot_[ASSOC_NUM];
        

        bucket () {
            state_.pack = 0;
        }

        void * operator new(size_t size) {
            return galc->malloc(size);
        }

        bool Get(_key_t key, _value_t& value) {
            for (size_t i = 0; i < ASSOC_NUM; ++i) {
                if (state_.read(i) && slot_[i].key == key) {
                    value = (_value_t)slot_[i].val;
                    return true;
                }
            }
            return false;
        }

        bool Insert(_key_t key, _value_t value) {
            if (state_.count() == ASSOC_NUM) {
                return false;
            } else {
                auto slotid = state_.alloc();
                slot_[slotid] = {key, (char*)value};
                clwb(&slot_[slotid], sizeof(Record));
                mfence();

                state_t new_state = state_;
                new_state.unpack.bitmap = state_.add(slotid);
                state_.pack = new_state.pack;
                clwb(&state_, 64);
                return true;
            }
        }

        bool Delete(_key_t key) {
            for (size_t i = 0; i < ASSOC_NUM; ++i) {
                if (state_.read(i) && slot_[i].key == key) {
                    state_t new_state = state_;
                    new_state.unpack.bitmap = state_.free(i);
                    state_.pack = new_state.pack;
                    clwb((void*)(&state_), 8);
                    return true;
                }
            }
            return false;
        }

    }__attribute__((aligned(PMLINE)));
    

    struct entrance
    {
        bucket*     buckets_[2];
        bucket*     interim_level_buckets_;
        uint64_t    level_;
    };
    


    class levelHash {
    public:
        levelHash(string path, bool recover, uint64_t level = 10) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "levelHash");
                addr_capacity_ = (uint64_t)1 << level;
                level_entry_num_[0] = 0;
                level_entry_num_[1] = 0;
                interim_level_buckets_ = nullptr;
                level_ = level;
                
                /* allocate*/
                entrance_ = (entrance *) galc->malloc(sizeof(entrance));
                buckets_[0] = (bucket *) galc->malloc(sizeof(bucket) * (addr_capacity_ + 1)); 
                buckets_[1] = (bucket *) galc->malloc(sizeof(bucket) * (addr_capacity_ / 2 + 1)); 

                /* persist */
                for (int i = 0; i <= addr_capacity_; ++i) {
                    clwb(&(buckets_[0][i].state_), 64);                    
                } 
                for (int i = 0; i <= addr_capacity_ / 2; ++i) {
                    clwb(&(buckets_[1][i].state_), 64);                    
                } 
                mfence();
                entrance_->buckets_[0] = galc->relative(buckets_[0]);
                entrance_->buckets_[1] = galc->relative(buckets_[1]);
                entrance_->interim_level_buckets_ = nullptr;
                entrance_->level_ = level;
                clwb(entrance_, sizeof(entrance));

            } else {
                galc = new PMAllocator(path.c_str(), true, "levelHash");
            }
        }
        
        ~levelHash() {

        }
        
        bool Get(_key_t key, _value_t& value) {
            uint64_t f_hash = hash1(key);
            uint64_t s_hash = hash2(key);
            uint64_t f_index = F_IDX(f_hash, addr_capacity_);
            uint64_t s_index = S_IDX(s_hash, addr_capacity_);

            for (int i = 0; i < 2; ++i) {
                if (buckets_[i][f_index].Get(key, value)) {
                    return true;
                }
                if (buckets_[i][s_index].Get(key, value)) {
                    return true;
                }
                f_index = F_IDX(f_hash, addr_capacity_ / 2);
                s_index = S_IDX(s_hash, addr_capacity_ / 2);
            }

            return false;
        }

        bool Insert(_key_t key, _value_t value) {
        Redo:
            uint64_t f_hash = hash1(key);
            uint64_t s_hash = hash2(key);
            uint64_t f_index = F_IDX(f_hash, addr_capacity_);
            uint64_t s_index = S_IDX(s_hash, addr_capacity_);

            for (int i = 0; i < 2; ++i) {
                if (buckets_[i][f_index].Insert(key, value)) {
                    return true;
                }
                if (buckets_[i][s_index].Insert(key, value)) {
                    return true;
                }
                f_index = F_IDX(f_hash, addr_capacity_ / 2);
                s_index = S_IDX(s_hash, addr_capacity_ / 2);
            }
            if (!Expand()) {
                return false;
            }
            goto Redo;
        }

        bool Delete(_key_t key) {

        }

    private:
        bool Expand() {
            uint64_t new_level = level_ + 1;
            uint64_t new_addr_capacity = (uint64_t)1 << new_level;
            
            /* allocate and persist */
            interim_level_buckets_ = (bucket*) galc->malloc(sizeof(bucket) * (new_addr_capacity + 1));
            entrance_->interim_level_buckets_ = galc->relative(interim_level_buckets_);
            clwb(entrance_->interim_level_buckets_, sizeof(bucket));
            mfence();

            /* copy entries */
            for (int i = 0; i < addr_capacity_ / 2; i++) {
                for (int j = 0; j < ASSOC_NUM; ++j) {
                    if (buckets_[1][i].state_.read(j)) {
                        _key_t  key = buckets_[1][i].slot_[j].key;
                        _value_t value = (_value_t)buckets_[1][i].slot_[j].val;
                        uint64_t f_hash = hash1(key);
                        uint64_t s_hash = hash2(key);
                        uint64_t f_index = F_IDX(f_hash, new_addr_capacity);
                        uint64_t s_index = S_IDX(s_hash, new_addr_capacity);
                        if (!interim_level_buckets_[f_index].Insert(key, value)) {
                            interim_level_buckets_[s_index].Insert(key, value);
                        }
                    }
                }
            }

            /* persist */
            for (int i = 0; i <= new_addr_capacity; ++i) {
                clwb(&(interim_level_buckets_[i].state_), 64);                    
            } 
            mfence();

            /* pointers consistency */
            buckets_[1] = buckets_[0];
            buckets_[0] = interim_level_buckets_;
            entrance_->buckets_[1] = entrance_->buckets_[0];
            clwb(entrance_->buckets_[1], sizeof(bucket));
            mfence();
            entrance_->buckets_[0] = entrance_->interim_level_buckets_;
            clwb(entrance_->buckets_[0], sizeof(bucket));
            mfence();
            entrance_->interim_level_buckets_ = nullptr;
            clwb(entrance_->interim_level_buckets_, sizeof(bucket));
        }    


    private:
        entrance*   entrance_;
        bucket*     buckets_[2];
        bucket*     interim_level_buckets_;
        
        uint64_t    level_;
        uint64_t    addr_capacity_;
        uint64_t    level_entry_num_[2];
    };
}




