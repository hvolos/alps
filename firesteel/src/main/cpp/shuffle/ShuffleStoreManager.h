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

#ifndef SHUFFLESTOREMANAGER_H_
#define SHUFFLESTOREMANAGER_H_

#include <string>
#include <vector>
#include "EnumKvTypes.h"

//NOTE: to remove cyclic, comment out these include files.
//#include "MapShuffleStoreWithIntKeys.h"
//#include "MapShuffleStoreWithStringKeys.h"

using namespace std;

//the following forward declaration is to break the cyclic dependency between ShuffleStoreManager
//and MapShuffleStoreManager, ReduceShuffleStoreManager, ByteBufferPool, and ArrayBufferPool
class ByteBufferPool;
class ArrayBufferPool;
class MapShuffleStoreManager;
class ReduceShuffleStoreManager;
//the NVM memory manager, either using the default one, 
class ShuffleDataSharedMemoryManager; 


class ShuffleStoreManager {
 private:
     static ShuffleStoreManager* m_pInstance; 
     //later we will have reduce store as well.

 private: 
     MapShuffleStoreManager  *mapShuffleStoreManager; 
     ReduceShuffleStoreManager *reduceShuffleStoreManager;
     //introduce the byte buffer pool 
     ByteBufferPool  *byteBufferPool; 

     //introduce the key buffer pool for map shuffle store's key buffering.
     //NOTE that keys are with different key types (int, long, double...), and we
     //will need to have a seperate key buffer pool.
     vector<ArrayBufferPool*> keyBufferPools; 

     //introduce the key buffer pool for reduce shuffle store's linked value  buffering
     //used for hash-merge.
     ArrayBufferPool   *linkedValueBufferPool;

     //NVM memory manager 
     ShuffleDataSharedMemoryManager *shuffleDataSharedMemoryManager; 
     //The NVM memory manager related information 
     string globalHeapName;  //global heap name
     int executorId;  //the executor id passed in from spark core runtime
     //generation id in RMB is designed for future allocated shared memory blocks clean up
     uint64_t generationId;
     //the virtual address memory for the allocated shared-memory region in this process
     pair<size_t, size_t> processMap;     
      
 public: 

      //due to  class forwarding, put to the implementation file
      ShuffleStoreManager();

      //due to  class forwarding, put to the implementation file
      ~ShuffleStoreManager();

     //there is no locking here. this method is desigend to be call from Java, and from Java 
     //side, we have to make sure that the first call to this method is via the single-threaded
     //environment. In this case, it is called via SparkEnv's creation of ShmShuffleManager.
     static ShuffleStoreManager* getInstance() {
       if(m_pInstance==nullptr) {
  	 m_pInstance = new ShuffleStoreManager();
       }
       return m_pInstance;
     };
    
     MapShuffleStoreManager* getMapShuffleStoreManager() {
       return mapShuffleStoreManager; 
     }

     ReduceShuffleStoreManager* getReduceShuffleStoreManager() {
       return reduceShuffleStoreManager; 
     }

     ByteBufferPool  *getByteBufferPool () {
       return byteBufferPool; 
     }

     //the passed-in is the enum type that I already defined the value range
     ArrayBufferPool  *getKeyBufferPool (KValueTypeId tid) {
       size_t keytypeId = tid;
       return keyBufferPools[keytypeId]; 
     }

     ArrayBufferPool   *getLinkedValueBufferPool() {
        return linkedValueBufferPool;
     }

     ShuffleDataSharedMemoryManager *getShuffleDataSharedMemoryManager() {
        return shuffleDataSharedMemoryManager; 
     }


     void initialize(const string& heapName, int exeId);

     //this is passed in at the iniitialiation time
     const string&  getGlobalHeapName () const {
       return globalHeapName; 
     }

     //this is passed in at the iniitialiation time
     int getExecutorId () const {
       return  executorId;
     }

     uint64_t getGenerationId() const {
       return  generationId;
     }

     pair<size_t, size_t> getProcessMap() const {
       return  processMap;
     }

     void shutdown();


     //to make sure that the global pointer, when mapping back into the process address
     //space, it falls into the process map range 
     bool check_pmap (void *p) const {
       size_t addr = reinterpret_cast <size_t> (p);
       bool result = (addr >= processMap.first) && (addr < processMap.second);
       return result;
     }   
  
     //NOTE: the clean up will be issued from Java for each created map store.
     //to shutdown all of the map stores that belong to the specified shuffle Id.
     //note that some of the map store may not belong to this executor, as map tasks
     //get distributed across executors. and this call is originated from a broadcast
     //call to the all of the shuffle manager on all of the executors.
     //void cleanup(int shuffleId, int numOfMaps);

     //format the shared-memory, with the path to the in-memory file system, and 
     //instance id captured when the heap is opened.
     void format_shm ();

     //for testing purpose. as the RMB heap instance does not function well after close and
     // format and open again.
     void register_new_shm();

};


#endif 
