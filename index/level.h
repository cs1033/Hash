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
#define ASSOC_NUM 14

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
        char fingerprints_[ASSOC_NUM];
        Record slot_[ASSOC_NUM];
        char dumnt_[16];

        bucket () {
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

        }

        bool Delete(_key_t key) {
            char fp = finger_print(key);
            for (size_t i = 0; i < ASSOC_NUM; ++i) {
                if (state_.read(i) && fingerprints_[i] == fp && slot_[i].key == key) {
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
    

    class levelHash {
    public:
        levelHash(string path, bool recover) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "levelHash");
            } else {
                galc = new PMAllocator(path.c_str(), true, "levelHash");
            }
        }
        
        ~levelHash() {

        }
        
        bool Get(_key_t key, _value_t& value) {

        }

        bool Insert(_key_t key, _value_t value) {

        }

        bool Delete(_key_t key) {

        }
    };
}




