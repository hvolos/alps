/* 
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _ALPS_LAYER_SLABHEAP_HH_
#define _ALPS_LAYER_SLABHEAP_HH_

#include "alps/common/assert_nd.hh"

#include "alps/layers/bits/slab.hh"

namespace alps {

/**
 * @brief Slab heap organizes slabs in per-sizeclass free lists 
 *
 * @details 
 * This class methods are not-thread safe. User is responsible for proper
 * serialization via lock/unlock.
 * 
 */
template<template<typename> class TPtr, template<typename> class PPtr>
class SlabHeap
{
public:
    typedef Slab<TPtr, PPtr> SlabT;
public:
    SlabHeap()
    { 
        ASSERT_ND(pthread_mutex_init(&mutex_, NULL) == 0);
    }

#if 0
    //malloc();
    ErrorCode malloc(size_t size_bytes, TPtr<void>* ptr);
    //free();
    //get();
    //put();
#endif

    TPtr<void> alloc_block(SlabT* slab)
    {
        TPtr<void> ptr;

        bool empty = slab->empty();
        int old_fullness = slab->fullness();
        ptr = slab->alloc_block();
        if (!ptr) {
            int new_fullness = slab->fullness();
            int szclass = slab->sizeclass();
            LOG(info) << "Allocate block: " << "new_fullness: " << new_fullness << " old_fullness: " << old_fullness;
            if (empty || (new_fullness != old_fullness)) {
                move_slab(slab, szclass, new_fullness);
            }
        }
        return ptr;
    }

    void free_block(SlabT* slab, TPtr<void> ptr)
    {
        int old_fullness = slab->fullness();
        slab->free_block(ptr);
        if (slab->empty()) {
            LOG(info) << "Free block: " << "recycle now empty slab";
            slab->remove();
            insert_slab_to_empty(slab);
        } else {
            int new_fullness = slab->fullness();
            int szclass = slab->sizeclass();
            LOG(info) << "Free block: " << "new_fullness: " << new_fullness << " old_fullness: " << old_fullness;
            move_slab(slab, szclass, new_fullness);
        }
    }

    SlabT* find_slab(const int szclass)
    {
        SlabT* slab = NULL;

        // Skip kSlabFullnessBins-1 slab list as it contains completely full slabs
        int fullness_hint = kSlabFullnessBins-2;

        // Find the most-full slab 
        for (int i=fullness_hint; i>=0; i--) {
            typename SlabT::SlabList& sl = full_slabs_[szclass][i];
            if (sl.size()) {
                slab = sl.front();
                break;
            }
        }

        if (!slab) {
            slab = reuse_empty_slab(szclass);
        }
        return slab;
    }

    SlabT* insert_slab(TPtr<nvSlab<TPtr>> nvslab)
    {
        LOG(info) << "Insert slab: " << nvslab;

        SlabT* slab = new SlabT(nvslab);
        insert_slab(slab, nvslab->sizeclass());
        return slab;
    }


    void insert_slab(SlabT* slab, int szclass)
    {
        slab->set_owner(this);

        int fullness = slab->fullness();

        if (slab->empty()) {
            insert_slab_to_empty(slab);
        } else {
            slab->insert(&full_slabs_[szclass][fullness]);
        }
    }


    void remove_slab(SlabT* slab)
    {
        slab->set_owner(NULL);
        slab->remove();
    }

    void move_slab(SlabT* slab, int szclass, int to)
    {
        slab->remove();
        slab->insert(&full_slabs_[szclass][to]);
    }

    void stream_to(std::ostream& os) const
    {
        for (int i=0; i<kSlabFullnessBins; i++) {
            std::cout << "[" << i << "] ";
            bool comma = false;
            for (int c=0; c<kSizeClasses; c++) {
                for (typename SlabT::SlabList::const_iterator it = full_slabs_[c][i].begin();
                     it != full_slabs_[c][i].end();
                     it++) 
                {
                    if (comma) { std::cout << ", "; }
                    std::cout << **it;
                    comma = true;
                }
            }
            std::cout << std::endl;
        }
        std::cout << "[R] ";
        bool comma = false;
        for (typename SlabT::SlabList::const_iterator it = empty_slabs_.begin();
             it != empty_slabs_.end();
             it++) 
        {
            if (comma) { std::cout << ", "; }
            std::cout << **it;
            comma = true;
        }
        std::cout << std::endl;
    }


    friend std::ostream& operator<<(std::ostream& os, const SlabHeap& slabheap)
    {
        slabheap.stream_to(os);
        return os;
    }

    void lock()
    {
        pthread_mutex_lock(&mutex_);
    }

    void unlock()
    {
        pthread_mutex_unlock(&mutex_);
    }

private:
    void insert_slab_to_empty(SlabT* slab)
    {
        slab->insert(&empty_slabs_);
    }

    SlabT* reuse_empty_slab(int szclass)
    {
        SlabT* slab = NULL;
        if (empty_slabs_.size()) {
            slab = empty_slabs_.front();
            int fullness = slab->fullness();
            move_slab(slab, szclass, fullness);
            if (slab->sizeclass() != szclass) {
                slab->init(szclass);
            }
        }
        return slab;
    }


protected:
    pthread_mutex_t          mutex_;
    typename SlabT::SlabList full_slabs_[kSizeClasses][kSlabFullnessBins]; // completely or partially full slabs
    typename SlabT::SlabList empty_slabs_; // completely empty slabs (that can be reused as a different size class)
};

} // namespace alps

#endif // _ALPS_LAYER_SLABHEAP_HH_
