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


namespace level {
    using std::string;
    using std::cout;
    using std::endl;


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




