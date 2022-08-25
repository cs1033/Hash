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


namespace linear {
    using std::string;
    using std::cout;
    using std::endl;
    
    const uint64_t ASSOC_NUM = 13;
    

    struct  bucket
    {
        state_t     state_;
        char        fingerprints_[ASSOC_NUM];
        char        dumnp_;
        Record      slot_[ASSOC_NUM];
        bucket*     next_[2];
        bucket*     append_[2];


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
                if (append_[0] != nullptr) {
                    bucket* append = (bucket*) galc->absolute(append_[0]);
                    return append->Insert(key, value);
                } else {
                    bucket* append = (bucket*) galc->malloc(sizeof(bucket));
                    append->InsertWithoutClwb(key, value, fp);
                    append->append_[0] = nullptr;
                    clwb(append, sizeof(bucket));
                    mfence();
                    append_[0] = galc->relative(append);
                    clwb(append_[0], 8);
                    return true;
                }
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

    }__attribute__((aligned(PMLINE)));


    struct directory
    {
        bucket** buckets_;         
    }__attribute__((aligned(CACHE_LINE_SIZE)));


    class linearHash {
        linearHash(string path, bool recover) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "linearHash");
            } else {

            }
        }
    };


}


