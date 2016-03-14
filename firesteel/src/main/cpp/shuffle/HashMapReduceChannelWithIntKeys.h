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

#ifndef  _HASH_MAP_BASED_REDUCE_CHANNEL_WITH_INT_KEY_H_
#define  _HASH_MAP_BASED_REDUCE_CHANNEL_WITH_INT_KEY_H_

#include  "GenericReduceChannel.h"
#include "ShuffleConstants.h"
#include "ExtensibleByteBuffers.h"
#include "HashMapKeyPositionTracker.h"
#include "MergeSortKeyPositionTrackerWithIntKeys.h"

#include <vector>
#include <unordered_map> 

using namespace std; 

/*
 *The wrapper on each MapBucket to keep track of the current cursor  and current value 
 *
*/
class  HashMapReduceChannelWithIntKeys: public GenericReduceChannel{
private:   
	int  kvalueCursor;
	int  currentKeyValue; 
	//unsigned char *currentValueValue; //retrieved with the current key 
	PositionInExtensibleByteBuffer currentValueValue; //use the value tracker in the buffer manager. 
	int currentValueSize; //the size of the current value 
	//buffer manager, as passed in.
	ExtensibleByteBuffers  *bufferMgr;

public: 
	
	HashMapReduceChannelWithIntKeys (MapBucket &sourceBucket, int rId, int rPartitions,
                ExtensibleByteBuffers * bufMgr) :
	        GenericReduceChannel(sourceBucket, rId, rPartitions),
		kvalueCursor(0),
		currentKeyValue(0),
		currentValueValue(-1, -1, 0),
		currentValueSize(0), 
		bufferMgr (bufMgr) {
		 //NOTE: total length passed from the reducer's mapbucket is just an approximation, 
                 //as when map status passed to the scheduler, bucket size compression happens.
                 //this total length gets recoverred precisely, at the reduce channel init () call.

	}

	~HashMapReduceChannelWithIntKeys() {
		//do nothing. 
	}

	/*
	 * to decide whether this channel is finished the scanning or not. 
	 */ 
	bool hasNext() {
		return (totalBytesScanned < totalLength); 
	}

	void getNextKeyValuePair(); 

	/*
	 * return the current key's value
	 */
	int getCurrentKeyValue() {
		return currentKeyValue; 
	}
	
	/*
	* return the current value corresponding to the current key. 
	*/
	PositionInExtensibleByteBuffer getCurrentValueValue() {
		return currentValueValue; 
	}

	/*
	* return the current value's size. 
	*/
	int getCurrentValueSize() {
		return currentValueSize; 
	}

	/*
	 * to shudown the channel and release the necessary resources, including the values created 
         *from malloc. 
	 */
	void shutdown() override {
	    //WARNING: this is not an efficient way to do the work. we will have to use the big buffer
            //to do memory copy,
	    //instead of keep doing malloc. we will have to improve this.
	    //for (auto p = allocatedValues.begin(); p != allocatedValues.end(); ++p) {
		//	free(*p);
	    //}
		
	}
};


class HashMapReduceEngineWithIntKeys {
private:
	vector <HashMapReduceChannelWithIntKeys>  hashMapReduceChannels;
	int reduceId;
	int totalNumberOfPartitions;  

	int currentMergedKey; 

	//buffer manager as passed in
	ExtensibleByteBuffers *bufferMgr; 
       
private:
	//key: the passed-in integer key.
        //value: the position in the hash-map tracker. start with zero capacity.
        //we will like to see whether it is better to start with same initial value
        
        //NOTE: consider Google HashTable whether there is API to clean up the current key/values, but
        //keep the allocated memory for future re-use?
	unordered_map<int, int> hashMergeTable; 

        //link list auxillary to the hash table, as map can only hold one element
	KeyWithFixedLength::HashMapValueLinkingWithSameKey  valueLinkList;
    
        int totalRetrievedKeyElements;
        //this needs to be set before key/values retrieval starts.
        int hashMergeTableSize; 
        //this will be set before key/values retrieval starts.
        unordered_map<int, int>::iterator hashMapIterator;
public: 

	//passed in: the reducer id and the total number of the partitions for the reduce side.
	HashMapReduceEngineWithIntKeys(int rId, int rPartitions, ExtensibleByteBuffers *bufMgr) :
		reduceId(rId), totalNumberOfPartitions(rPartitions), currentMergedKey(-1),
		  bufferMgr(bufMgr),
                  hashMergeTable(SHMShuffleGlobalConstants::HASHMAP_INITIALIAL_CAPACITY),
                  valueLinkList(rId),
                  totalRetrievedKeyElements(0),
                  hashMergeTableSize (0)
                  {
		    //do nothing
	}

	/*
	 * to add the channel for merge sort, passing in the map bucket. 
	 */
	void addHashMapReduceChannel(MapBucket &mapBucket) {
	    HashMapReduceChannelWithIntKeys  channel(mapBucket, 
                                                  reduceId, totalNumberOfPartitions, bufferMgr);
    	    hashMapReduceChannels.push_back(channel);
	}

	/*
	 * to init the hash-map merge engine 
	 */
	void init(); 


        /*
         * to reset the buffer manager buffer to the beginning, for next key/values pair retrieval
         */
        //void reset_buffermgr() {
	//  bufferMgr->reset();
	//}
          
	/*
	* to decide whether this channel is finished the scanning or not.
	*/
	bool hasNext() {
	  return (totalRetrievedKeyElements < (int)hashMergeTableSize);
	}

        //NOTE: we still use the mergesorted map buckets as the result first. we will change the name of 
        //mergesorted map buckets later.
	void getNextKeyValuesPair(IntKeyWithFixedLength::MergeSortedMapBuckets& mergedResultHolder);


	/*
	 *for all of channels to be merged, to get the next unique key.  For example, key =  198. 
	 */
	int  getCurrentMergedKey() {
		return currentMergedKey; 
	}

        //to release the DRAM related resources.
        void shutdown(); 
};


#endif /*_HASH_MAP_BASED_REDUCE_CHANNEL_WITH_INT_KEY_H_*/
