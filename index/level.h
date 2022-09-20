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


#define F_IDX(hash, capacity) (hash % (capacity / 2))
#define S_IDX(hash, capacity) ((hash % (capacity / 2)) + (capacity / 2))

namespace level {
    using std::string;
    using std::cout;
    using std::endl;

    const uint64_t ASSOC_NUM = 15;

    

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
                slot_[slotid] = {key, value};
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
        bucket*     buckets_[2][2];
        bucket*     interim_level_buckets_[2];
        uint64_t    level_[2];
        uint8_t     version_;
    };
    


    class levelHash {
    public:
        levelHash(string path, bool recover, uint64_t level = 10) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "levelHash");
                addr_capacity_ = (uint64_t)1ull << level;
                level_entry_num_[0] = 0;
                level_entry_num_[1] = 0;
                interim_level_buckets_ = nullptr;
                level_ = level;
                
                /* allocate*/
                entrance_ = (entrance *) galc->get_root(sizeof(entrance));
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
                entrance_->buckets_[0][0] = galc->relative(buckets_[0]);
                entrance_->buckets_[0][1] = galc->relative(buckets_[1]);
                entrance_->interim_level_buckets_[0] = nullptr;
                entrance_->interim_level_buckets_[1] = nullptr;
                entrance_->level_[0] = level;
                entrance_->version_ = 0;
                clwb(entrance_, sizeof(entrance));

            } else {
                galc = new PMAllocator(path.c_str(), true, "levelHash");
                entrance_ = (entrance *) galc->get_root(sizeof(entrance));

                auto version = entrance_->version_;
                buckets_[0] = galc->absolute(entrance_->buckets_[version][0]);
                buckets_[1] = galc->absolute(entrance_->buckets_[version][1]);
                interim_level_buckets_ = galc->absolute(entrance_->interim_level_buckets_[version]);
                level_ = entrance_->level_[version];

            }
        }
        
        ~levelHash() {
            std::cout << "level: " << level_ << std::endl;
            // std::cout << "capacity: " << addr_capacity_ << std::endl;
            // std::cout << "level 0: " << level_entry_num_[0] << std::endl;
            // std::cout << "level 1: " << level_entry_num_[1] << std::endl;
        }
        
        bool Get(_key_t key, _value_t& value) {
            uint64_t f_hash = hash1(key);
            uint64_t s_hash = hash2(key);
            uint64_t f_index = F_IDX(f_hash, addr_capacity_);
            uint64_t s_index = S_IDX(s_hash, addr_capacity_);

            if (level_entry_num_[0] >= level_entry_num_[1]) {   /* top has more entries */
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
            } else {                                            /*  bottom has more entries */
                f_index = F_IDX(f_hash, addr_capacity_ / 2);
                s_index = S_IDX(s_hash, addr_capacity_ / 2);
                for (int i = 1; i >= 0; --i) {
                    if (buckets_[i][f_index].Get(key, value)) {
                        return true;
                    }
                    if (buckets_[i][s_index].Get(key, value)) {
                        return true;
                    }
                    f_index = F_IDX(f_hash, addr_capacity_);
                    s_index = S_IDX(s_hash, addr_capacity_);
                }
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
                    level_entry_num_[i]++;
                    return true;
                }
                if (buckets_[i][s_index].Insert(key, value)) {
                    level_entry_num_[i]++;
                    return true;
                }
                f_index = F_IDX(f_hash, addr_capacity_ / 2);
                s_index = S_IDX(s_hash, addr_capacity_ / 2);
            }

            /* try move an entry */
            f_index = F_IDX(f_hash, addr_capacity_);
            s_index = S_IDX(s_hash, addr_capacity_);
            for (int i = 0; i < 2; ++i) {
                if (TryMovement(i, f_index)) {
                    return buckets_[i][f_index].Insert(key, value);
                }
                if (TryMovement(i, s_index)) {
                    return buckets_[i][s_index].Insert(key, value);
                }
                f_index = F_IDX(f_hash, addr_capacity_ / 2);
                s_index = S_IDX(s_hash, addr_capacity_ / 2);
            }

            Expand();
            goto Redo;
        }

        bool Delete(_key_t key) {
            return true;
        }

    private:
        bool TryMovement(int level, uint64_t index) {
            for (int i = 0; i < ASSOC_NUM; ++i) {
                _key_t      new_key = buckets_[level][index].slot_[i].key;
                _value_t    new_value = buckets_[level][index].slot_[i].val;
                uint64_t    f_hash = hash1(new_key);
                uint64_t    s_hash = hash2(new_key);
                uint64_t    f_index = F_IDX(f_hash, addr_capacity_);
                uint64_t    s_index = S_IDX(s_hash, addr_capacity_);
                uint64_t    k_index = index == f_index ? s_index : f_index;

                if (buckets_[level][k_index].Insert(new_key, new_value)) {
                    buckets_[level][index].Delete(new_key);
                    return true;
                }
            }
            return false;
        }

        bool Expand() {
            uint8_t new_version = (entrance_->version_ + 1) % 2;
            uint64_t new_level = level_ + 1;
            uint64_t new_addr_capacity = 1ull << new_level;
            addr_capacity_ = new_addr_capacity;
            level_ = new_level;
            std::swap(level_entry_num_[0], level_entry_num_[1]);
            
            /* allocate and persist */
            interim_level_buckets_ = (bucket*) galc->malloc(sizeof(bucket) * (new_addr_capacity + 1));
            entrance_->interim_level_buckets_[new_version] = galc->relative(interim_level_buckets_);
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
            galc->free(buckets_[1]);
            buckets_[1] = buckets_[0];
            buckets_[0] = interim_level_buckets_;
            entrance_->buckets_[new_version][0] = galc->relative(buckets_[0]);
            entrance_->buckets_[new_version][1] = galc->relative(buckets_[1]);
            entrance_->interim_level_buckets_[new_version] = nullptr;
            entrance_->level_[new_version] = level_;
            clwb(entrance_, sizeof(entrance));
            mfence();

            entrance_->version_ = new_version;
            clwb(&(entrance_->version_), 8);

            return true;
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




