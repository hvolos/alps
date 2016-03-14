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

#ifndef REDUCESHUFFLESTORE_WITH_STRING_KEYS_H_
#define REDUCESHUFFLESTORE_WITH_STRING_KEYS_H_

#include <vector>
#include "ExtensibleByteBuffers.h"
#include "MergeSortReduceChannelWithStringKeys.h"
#include "ShuffleConstants.h"
#include "MapStatus.h"
#include "GenericReduceShuffleStore.h"

using namespace std;

//to define the structure that only is with variable length key.
namespace KeyWithVariableLength {

  /*
   * this is designed for testing purpose, not for actual merge-sort shuffle.
   *
   */
  struct RetrievedMapBucket {
    int reducerId; 
    int mapId; //which bucket in th specified map to be retrieved; 
    vector <unsigned char* > keys;
    //we need key size, as I do not want to use "string" for value passing.
    vector <int> keySizes;
    vector <unsigned char* > values;
    vector <int> valueSizes;

    RetrievedMapBucket(int rId, int mId): reducerId (rId), mapId (mId) {
      //
    }

    vector<unsigned char*>& get_keys() {
      return keys;
    }

    vector <unsigned char* > & get_values() {
      return values;
    }

    vector <int> & get_keySizes() {
      return keySizes;
    }

    vector <int> & get_valueSizes () {
      return valueSizes;
    }
  };

 /*
  * this is designed for real merge-sort at the reducer side.
  */
 struct MergeSortedMapBuckets {
    int reducerId; 
    //the key is with the variable length
    vector <PositionInExtensibleByteBuffer> keys; 
    vector <int> keySizes;   

    //NOTE: for performance, should this be considered with "move" operator?
    vector <vector <PositionInExtensibleByteBuffer>> kvaluesGroups;
    //we will need the size to de-serialize the byte array. 
    vector <vector <int>> kvaluesGroupSizes;

    MergeSortedMapBuckets(int rId) : reducerId(rId) {

   }
  };
};

class  ReduceShuffleStoreWithStringKey: public GenericReduceShuffleStore {
 private:
     //Note: this becomes a local copy.
     ReduceStatus reduceStatus; 
     int totalNumberOfPartitions; 
     int reducerId; 
 
     //the buffer manager, held at the ReduceShuffleStoreManager.
     //same life time as ReduceShuffleStoreWithStringKeys.
     BufferManager *kvBufferMgr; 

     MergeSortReduceEngineWithStringKeys theMergeSortEngine;
     bool engineInitialized;

     //for key value type definition
     KValueTypeDefinition kvTypeDefinition;

     //value type definition
     VValueTypeDefinition vvTypeDefinition;

     //to specify whether the reduce-side needs key ordering or not:
     //if key ordering is required, we will use merge-sort to merge sorted data from the map side.
     //otherwise, we will use hash map based merge without taking into account ordering
     bool orderingRequired;

 public: 

	 ReduceShuffleStoreWithStringKey(const ReduceStatus &status,
				      int partitions, int redId, 
					 BufferManager *kvMgr, bool ordering) :
		 reduceStatus(status), totalNumberOfPartitions(partitions), reducerId(redId),
		 kvBufferMgr(kvMgr),
		 theMergeSortEngine(redId, partitions, kvMgr),
		 engineInitialized (false), kvTypeDefinition(KValueTypeId::String),
		 orderingRequired(ordering)  {

	 }

	 //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function
         virtual ~ReduceShuffleStoreWithStringKey () {
           //do nothing
         }
    
         //retrieve reducer id
	 int getReducerId() {
	    return reducerId;
	 }

	 //for testing purpose 
	 ReduceStatus& getReduceStatus() {
		return reduceStatus; 
	 }

	 //to expose the buffer manager. 
	 BufferManager* getKvBufferMgr() {
		return kvBufferMgr; 
	 }

         MergeSortReduceEngineWithStringKeys& getMergeSortEngine() {
	        return theMergeSortEngine;  
	 }
      
         void setVVTypeDefinition(const VValueTypeDefinition &def){
 	       //value copy
	   vvTypeDefinition = def;
	 }

         VValueTypeDefinition  getVValueType () override {
	       return vvTypeDefinition;
	 }

	 KValueTypeDefinition getKValueType() override {
	   return kvTypeDefinition;
         }

	 bool needsOrdering() override {
	   return orderingRequired;
         }

        //for testing purpose.to retrieve only the bucket contributed from the specified mapId; 
	KeyWithVariableLength::RetrievedMapBucket retrieve_mapbucket(int mapId); 
        void free_retrieved_mapbucket(KeyWithVariableLength::RetrievedMapBucket &mapBucket);

        //for testing purpose. to retrieve all of the map buckets and aggregate the key with 
        //the values, for all of the buckets that belong to the same reducer id. 
	KeyWithVariableLength::MergeSortedMapBuckets retrieve_mergesortedmapbuckets () ;


        //for real key/value pair retrieval 
        void init_mergesort_engine();

	//for real key/value pair retrieval, with the specified number of keys to be retrieved 
        //at a given time.
        //return the actual number of k-vs obtained.
        int retrieve_mergesortedmapbuckets (int max_number, 
                          KeyWithVariableLength::MergeSortedMapBuckets& resultHolder);

        //NOTE: who will issue the clean up of the intermediate results. It seems 
        //that is when the last kv pair gets pull out. 
        void stop() override;

        //NOTE: do we have shutdown?
        void shutdown() override;
};

#endif 
