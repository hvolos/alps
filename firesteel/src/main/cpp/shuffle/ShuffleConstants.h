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

#ifndef SHM_SHUFFLE_CONSTANTS_H
#define SHM_SHUFFLE_CONSTANTS_H

class SHMShuffleGlobalConstants {
public:
     //to switch between the default malloc and the Retail-Memory-Broker related memory management.
     static const bool USING_RMB = true;

     static const  int NORMALIZED_KEY_SIZE = 8;
     static const  size_t  BYTEBUFFER_HOLDER_SIZE= 4*1024*1024; // in KB.
     static const  size_t  MAXIMUM_SIZE_OF_VALUE_TYPE_DEFINITION = 10240; //10 KB
     //1000 *4MB = 4GB for a single executor in Spark

     //the following two parameters are at the map side, to allow byte buffers to be reused by different maps 
     static const  size_t  MAXIMUM_BYTEBUFFER_POOL_SIZE = 1000 ;
     //to turn on byte buffer pool selectively 
     static const  bool  BYTEBUFFER_POOL_ON = true;


     //The following three parameters are at the map side: 
     //at the map side, the array size for the initial array holding key data at map shuffle store 
     //and use it for incremental growth of the buffer via re-alloc
     //NOTE: this is just the initial size, as the actual map computation goin on, the actual buffer will 
     //grow 
     static const  size_t MAPSHUFFLESTORE_KEY_BUFFER_SIZE=500000; //half million

     //this can be later configurable, depending on the number of concurrent tasks
     //running on one executor.
     //to turn on byte buffer pool selectively 
     //the following two are for map-side shuffle store's key pooling
     static const  bool  KEYBUFFER_POOL_ON = true;
     static const  size_t MAXIMUM_KEYBUFFER_POOL_SIZE=30; 
     //the following two are for reduce-side shuffle store's key pooling
     static const  bool  LINKVALUEBUFFER_POOL_ON = true;
     static const  size_t MAXIMUM_LINKVALUEBUFFER_POOL_SIZE=30; 

     //the following paramater are at the reduce side, for merge-sort, to serve as the buffer to hold the values 
     //coming out of merge sort channels
     //the actual size will grow later, depending on how the actual reducer behaves. 
     static const  size_t MERGESORTED_KEY_TRACKER_SIZE=50000;
     static const  size_t MERGESORTED_POSITIONBUFFER_TRACKER_SIZE=50000;
    
     //the following paramater are at the reduce side, for hash-map based mergeing
     static const  size_t HASHMAP_VALUE_TRACKER_SIZE=500000;
     static const  size_t HASHMAP_INITIALIAL_CAPACITY=500000;

     //the following paramater are at the reduce side, for pass-through nothing merged
     //NOTE: this number should be the same as the Scala size as the batch size. If it is smaller. 
     //that is OK, as it can grow.
     static const size_t PASSTHROUGH_KEYVALUE_TRACKER_SIZE= 50000;

     //the thread-specific serialization/de-serialization buffer allocated for each thread,
     //and reused in the life time of the physical thread. 
     static const size_t SERIALIZATION_BUFFER_SIZE = 1*1024*1024*1024;


};

#endif 
