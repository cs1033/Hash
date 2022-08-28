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

#define  LSB


namespace linear {
    using std::string;
    using std::cout;
    using std::endl;
    
    const uint64_t  ASSOC_NUM = 13;
    const double    FILL_RATE = 0.8;
    

    struct  bucket
    {
        state_t     state_;
        char        fingerprints_[ASSOC_NUM];
        char        dumnp_;
        Record      slot_[ASSOC_NUM];
        bucket*     next_;
        bucket*     append_;


        bucket() {
            state_.pack = 0;
            next_ = nullptr;
            append_ = nullptr;
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
            if (append_ != nullptr) {
                return galc->absolute(append_)->Get(key, value);
            } else {
                return false;
            }
        }

        bool Insert(_key_t key, _value_t value) {
            char fp = finger_print(key);
            if (state_.count() == ASSOC_NUM) {
                if (append_ != nullptr) {
                    bucket* append = (bucket*) galc->absolute(append_);
                    return append->Insert(key, value);
                } else {
                    bucket* append = (bucket*) galc->malloc(sizeof(bucket));
                    append->InsertWithoutClwb(key, value, fp);
                    append->append_ = nullptr;
                    clwb(append, sizeof(bucket));
                    mfence();
                    append_ = galc->relative(append);
                    clwb(&append_, 8);
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
        uint64_t dir_size_, bucket_cnt_;
        uint64_t length_;         
    }__attribute__((aligned(CACHE_LINE_SIZE)));


    class linearHash {
    public:
        linearHash(string path, bool recover, uint64_t dir_size = LOADSCALE * 1024ull * 1024ull / ASSOC_NUM) {
            if (recover == false) {
                galc = new PMAllocator(path.c_str(), false, "linearHash");
                
                /* allocate*/
                dir_ = new directory();
                dir_->buckets_ = (bucket**)malloc(sizeof(bucket*) * dir_size);
                entrance_ = (bucket*) galc->get_root(sizeof(bucket));
                bucket* zero = (bucket*) galc->malloc(sizeof(bucket));

                /* assign */
                entry_cnt_ = 0;
                dir_->dir_size_ = dir_size;
                dir_->bucket_cnt_ = 1;
                dir_->length_ = 1;
                dir_->buckets_[0] = zero;
                entrance_->next_ = galc->relative(zero);
                zero->append_ = nullptr;
                zero->next_ = nullptr;

                /* persist  */
                clwb(zero, sizeof(bucket));
                clwb(entrance_, sizeof(bucket));

                cout << "init success " << endl;
            } else {

            }
        }

        ~linearHash() {

        }

        bool Get(_key_t key, _value_t value) {
            uint64_t f_hash = hash1(key);
          #ifdef LSB
            uint64_t and_value = (1ull << dir_->length_) - 1ull;
          #else
            //TODO
          #endif
            f_hash &= and_value;
            uint64_t f_index = f_hash >= dir_->bucket_cnt_ ? f_hash - (1ull << (dir_->length_ - 1ull)) : f_hash;
            bucket*  f_bucket = dir_->buckets_[f_index];

            return f_bucket->Get(key, value);
        }

        bool Insert(_key_t key, _value_t value) {
            /* allocate a new bucket */
            if ((0.0 + entry_cnt_) / (0.0 + dir_->bucket_cnt_) / ASSOC_NUM > FILL_RATE) {
                AllocateBucket();
            }

            uint64_t f_hash = hash1(key);
          #ifdef LSB
            uint64_t and_value = (1ull << dir_->length_) - 1ull;
          #else
            //TODO
          #endif
            f_hash &= and_value;
            uint64_t f_index = f_hash >= dir_->bucket_cnt_ ? f_hash - (1ull << (dir_->length_ - 1ull)) : f_hash;
            bucket*  f_bucket = dir_->buckets_[f_index];
            f_bucket->Insert(key, value);
            entry_cnt_++;
            return true;
        }

    private:
        void AllocateBucket() {
            /* update directory */
            if (dir_->dir_size_ == dir_->bucket_cnt_) {
                dir_->dir_size_ *= 2ull; 
                bucket** new_buckets = (bucket**)malloc(sizeof(bucket*) * dir_->dir_size_);
                for (int i = 0; i < dir_->bucket_cnt_; ++i) {
                    new_buckets[i] = dir_->buckets_[i];
                }
                free(dir_->buckets_);
                dir_->buckets_ = new_buckets;
            }
            dir_->bucket_cnt_++;
            if (dir_->bucket_cnt_ > (1ull << (dir_->length_))) {
                dir_->length_++;
            }

        
            /* allocate */
            uint64_t new_index = dir_->bucket_cnt_ - 1ull;
            bucket* new_bucket = (bucket*) galc->malloc(sizeof(bucket));
            bucket* replace_bucket = dir_->buckets_[new_index - (1ull << (dir_->length_ - 1ull))];
            bucket* last_bucket = dir_->buckets_[new_index - 1ull];

            // cout << "new_index:" << new_index << " replace_index:" << new_index - (1ull << (dir_->length_ - 1ull)) << endl;

            if (replace_bucket->append_ != nullptr) {    //multi buckets
                if (new_index == 47683) {
                    cout << "multi" << endl;
                }
                bucket* copy_bucket = (bucket*) galc->malloc(sizeof(bucket));
                bucket* zero = copy_bucket, *one = new_bucket;


                /* copy entries */
                while (1) {
                    for (int i = 0; i < ASSOC_NUM; ++i) {
                        if (replace_bucket->state_.read(i)) {
                            _key_t k = replace_bucket->slot_[i].key;
                            uint64_t hash = hash1(k);
                        #ifdef LSB
                            uint64_t and_value = (1ull << dir_->length_) - 1ull;
                        #else
                            //TODO
                        #endif
                            hash &= and_value;
                            uint64_t index = hash >= dir_->bucket_cnt_ ? hash - (1ull << (dir_->length_ - 1ull)) : hash;

                            if (index == new_index) {
                                if (one->state_.count() == ASSOC_NUM) {
                                    one->append_ = (bucket*) galc->relative(galc->malloc(sizeof(bucket)));
                                    clwb(one, sizeof(bucket));
                                    one = galc->absolute(one->append_);
                                } 
                                one->InsertWithoutClwb(k, replace_bucket->slot_[i].val, replace_bucket->fingerprints_[i]);
                            } else {
                                if (zero->state_.count() == ASSOC_NUM) {
                                    zero->append_ = (bucket*) galc->relative(galc->malloc(sizeof(bucket)));
                                    clwb(zero, sizeof(bucket));
                                    zero = galc->absolute(zero->append_);
                                } 
                                zero->InsertWithoutClwb(k, replace_bucket->slot_[i].val, replace_bucket->fingerprints_[i]);
                            }
                        }
                    }
                    if (replace_bucket->append_ != nullptr) {
                        replace_bucket = galc->absolute(replace_bucket->append_);
                    } else {
                        break;
                    }
                } // while
           

                bucket* replace_bucket = dir_->buckets_[new_index - (1ull << (dir_->length_ - 1ull))];

                /* add new bucket to bucket level */
                clwb(zero, sizeof(bucket));
                clwb(one, sizeof(bucket));
                clwb(new_bucket, sizeof(bucket));
                mfence();
                last_bucket->next_ = galc->relative(new_bucket);
                clwb(&(last_bucket->next_), 8);
                mfence();


                /* add copy bucket to bucket level */
                uint64_t replace_index = new_index - (1ull << (dir_->length_ - 1ull));
                bucket* pre_bucket = replace_index != 0ull ? dir_->buckets_[new_index - (1ull << (dir_->length_ - 1ull)) - 1ull] : entrance_;
                copy_bucket->next_ = replace_bucket->next_;
                clwb(copy_bucket, sizeof(bucket));
                mfence();
                pre_bucket->next_ = galc->relative(copy_bucket);
                clwb(&(pre_bucket->next_), 8);
                mfence();
                dir_->buckets_[replace_index] = copy_bucket;

                
                /* free replace bucket */
                bucket* tmp = replace_bucket;
                while (1) {
                    tmp = galc->absolute(replace_bucket->append_);
                    galc->free(replace_bucket);
                    replace_bucket = tmp;
                    if (replace_bucket->append_ == nullptr) {
                        break;
                    }
                }

            } else {    //single bucket
                /* move entries */
                state_t new_state = replace_bucket->state_;
                for (int i = 0; i < ASSOC_NUM; ++i) {
                    if (replace_bucket->state_.read(i)) {
                        _key_t k = replace_bucket->slot_[i].key;
                        uint64_t hash = hash1(k);
                      #ifdef LSB
                        uint64_t and_value = (1ull << dir_->length_) - 1ull;
                      #else
                        //TODO
                      #endif
                        hash &= and_value;
                        uint64_t index = hash >= dir_->bucket_cnt_ ? hash - (1ull << (dir_->length_ - 1ull)) : hash;

                        if (index == new_index) {
                            new_bucket->InsertWithoutClwb(k, replace_bucket->slot_[i].val, replace_bucket->fingerprints_[i]);
                            new_state.unpack.bitmap = new_state.free(i); 
                        }
                    }
                }
                /* add new bucket to bucket level */
                clwb(new_bucket, sizeof(bucket));
                mfence();
                last_bucket->next_ = galc->relative(new_bucket);
                clwb(&(last_bucket->next_), 8);
                mfence();

                /* delete duplicate entries */
                replace_bucket->state_ = new_state;
                clwb(&(replace_bucket->state_), 8);
            }

            /* add new bucket to directory */
            dir_->buckets_[new_index] = new_bucket;
            // cout << "success " << endl;

        }

    private:
        bucket*     entrance_;
        directory*  dir_;
        uint64_t    entry_cnt_;
    };


}


