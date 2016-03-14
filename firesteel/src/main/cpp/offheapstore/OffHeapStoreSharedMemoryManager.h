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
 *
 */

#ifndef OFFHEAP_STORE_SHARED_MEMORY_MANAGER_H
#define OFFHEAP_STORE_SHARED_MEMORY_MANAGER_H

using namespace std;

#include <glog/logging.h>
#include <stdlib.h>
#include <string>
#include  "common/os.hh"
#include "globalheap/globalheap.hh"

using namespace alps;

//the interface to support the default memory allocator and the one from RMB
class OffHeapStoreSharedMemoryManager {

 public:

     //to do initialization for the memory manager.
     virtual void initialize (const string &heapName) =0;
     virtual void shutdown() = 0;
     //NOTE: this will be replaced later by shared-memory oriented memory allocator
      //to allocate the data chunk for the offheap data. 
      //the number of the data chunks is specified by the nunmber of partitions
      //from the Spark execution engine 
     virtual RRegion::TPtr<void> allocate_datachunk(size_t size) =0;

     virtual void free_datachunk(RRegion::TPtr<void>& p) =0;

     //to retrieve the generation id associated with the current executor process.
     //for future clean-up purpose
     virtual uint64_t getGenerationId() const = 0;

     //to retrieve the mapped virtual address range in this process for the shm region
     virtual pair<size_t, size_t> get_pmap(const string &heapName) = 0;
     
     //for testing purpose
     virtual void format_shm (const string &heapName, GlobalHeap::InstanceId generationId) =0;
 
     virtual ~OffHeapStoreSharedMemoryManager () {
        //do nothing.
     }

};


class LocalOffHeapStoreSharedMemoryManager: public OffHeapStoreSharedMemoryManager {
   public:
      LocalOffHeapStoreSharedMemoryManager() {
	//do nothing.
      }

      virtual ~LocalOffHeapStoreSharedMemoryManager() {
	//do nothing.
      }

      void initialize (const string &heapName) override {
        //do nothing
      }

      void shutdown() override {
        //do nothing
      }

      RRegion::TPtr<void> allocate_datachunk (size_t size) override {
         void* nptr= malloc(size);
         return RRegion::TPtr<void> (-1, reinterpret_cast<uint64_t> (nptr));
      }

      void free_datachunk (RRegion::TPtr<void>& p)  override {
        void *nptr = reinterpret_cast <void*>(p.offset());
        free(nptr);
      }

      uint64_t getGenerationId() const override {
         LOG(WARNING) << "local memory allocator only supports generion id = 0";
         return 0;
      }

      void format_shm (const string &heapName, GlobalHeap::InstanceId generationId) override {
         LOG(WARNING) << "local memory allocator does not support formating of shm";
      }

      pair<size_t, size_t> get_pmap(const string &heapName) override {
         LOG(WARNING) << "local memory allocator does not support memory map retrieval of shm";
         pair<size_t, size_t> result (-1, -1);
         return result;
      }

     private: 
      //nothing
};


class RMBOffHeapStoreSharedMemoryManager: public OffHeapStoreSharedMemoryManager {

   private:
      GlobalHeap* heap = nullptr;
      bool heapOpen = false;
      string  logLevel;
      const string DEFAULT_LOG_LEVEL = "info";

   public:
      RMBOffHeapStoreSharedMemoryManager (){
        //retrieved the environment variable specified for rmb log level.
        //the default is info level
        char *logspec = getenv ("rmb_log");
        if (logspec != nullptr) {
          logLevel = logspec;
          LOG(INFO) << "RMB log level chosen is: " << logLevel;
        }
        else {
          logLevel = DEFAULT_LOG_LEVEL;
          LOG(INFO) << "RMB default log level chosen is: " << logLevel;
        }

        PegasusOptions pgopt;
        pgopt.debug_options.log_level = logLevel;
        Pegasus::init(&pgopt);
      }

      // the passed-in parameter heap name should be the full-path on the in-memory file system
      void initialize (const string &heapName) override {
         VLOG(2) << "rmb shared memory manager to be initialized for heap: "<<heapName.c_str() <<endl;
         GlobalHeap::open(heapName.c_str(), &heap);

         //WARNING: should I do the init?
         if (heap != nullptr) {
            heapOpen = true;
            LOG(INFO) << "rmb shared memory manager initialized with global heap:"<< (void*) heap << endl;
         }
         else {
            //Logging a FATAL message terminates the program (after the message is logged)
            LOG(FATAL) << "rmb shared memory manager failed to initialize the global heap: "
                       << heapName << endl;
         }

      }

      void shutdown() override {
        //what should we do the clean up.
        if (heap != nullptr) {
            heap->close();
            heapOpen = false;
            heap = nullptr;
        }

        VLOG(2) << "rmb shared memory manager shutdown"<<endl;
      }

      virtual ~RMBOffHeapStoreSharedMemoryManager () {
        //what should we do the clean up.
        if (heap != nullptr) {
            heap->close();
            heapOpen = true;
            heap = nullptr;
        }

        VLOG(2) << "rmb shared memory manager shutdown"<<endl;
      }

      //what is returned is the full virtual address on the current process
      RRegion::TPtr<void> allocate_datachunk (size_t size) override {
        if (heapOpen) {
           return heap->malloc(size);
        }
        else {
           RRegion::TPtr<void> null_ptr(-1, -1);
           return  null_ptr;
        }
      }

      void free_datachunk (RRegion::TPtr<void>& p) override {
        if (heapOpen) {
            heap->free(p);
        }
        else {
           LOG(ERROR) << "try to free data chunk from a closed heap";
        }
      }

      uint64_t getGenerationId() const override {
        if (heapOpen) {
          GlobalHeap::InstanceId instanceId = heap->instance();
          return instanceId;
        }
        else {
           LOG(ERROR) << "try to get generation id from a closed heap";
           return -1;
        }
      }

      //NOTE: an open heap can not be formated, as it contains the volatile data structures
      //cached from the persistent heap metadata store.
      //but once the heap is closed, generation id can not be queried. So generation id has to
      //be passed in, or cached earlier before the heap is closed.
      void format_shm (const string &heapName, GlobalHeap::InstanceId generationId) override {
        if (!heapOpen) {
          GlobalHeap::format_instance(heapName.c_str(), generationId);
        }
        else {
          LOG(ERROR) << "try to format an open heap, close it first...";
        }
      }

      //retrieve the map only when the heap is open
      pair<size_t, size_t> get_pmap(const string &heapName) override {
        if (heapOpen) {
          // this check assumes the heap file is mapped as a single contiguous memory region
          ProcessMap pmap;
          pair<size_t, size_t> pmap_range = pmap.range(heapName);
          return pmap_range;
        }
        else {
          LOG(ERROR) << "try to retrieve memory map when the heap is closed." ;
          pair<size_t, size_t> pmap_range (-1, -1);
          return pmap_range;
        }
      }
};
#endif  /*OFFHEAP_STORE_SHARED_MEMORY_MANAGER_H*/
