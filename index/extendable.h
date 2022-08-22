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
#define ASSOC_NUM 14

namespace extendable {
    using std::string;
    using std::cout;
    using std::endl;



    struct  bucket
    {
        state_t state_;
        char fingerprints_[ASSOC_NUM];
        // char dumnt_[14];
        Record slot_[ASSOC_NUM];
        bucket* next_;
    }__attribute__((aligned(PMLINE)));
    
}

