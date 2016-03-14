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

#ifndef MERGE_SORT_KEY_POSITION_TRACKER_WITH_INT_KEYS_H_
#define MERGE_SORT_KEY_POSITION_TRACKER_WITH_INT_KEYS_H_

#include <glog/logging.h>
#include "ShuffleConstants.h"
#include "ExtensibleByteBuffers.h"
#include <stdlib.h>
#include <string.h>

//to define the structure that only is with fixed length key.
namespace IntKeyWithFixedLength {
  
  //for a given key, what is the start and end in PositionInExtensibleByteBuffer array.
  struct MergeSortedKeyTracker {
    int key; 
    int start_position; //index to the array of PositionInExtensibleByteBuffer;
    int end_position;   //index to the array of PositionInExtensibleByteBuffer;
  };

  /*
   * this is designed for real merge-sort at the reducer side.
   */
  struct MergeSortedMapBuckets {
    int reducerId; 
    //the curent allocated number of elements to hold the key tracker.
    //therefore, total malloc will need keyTrackerCapacity*sizeof(MergeSoredKeyTracker)
    size_t keyTracker;
    size_t currentKeyTrackerCapacity; 
    //the curent allocated number of elements to hold PositionInExtensibielByteBuffer
    //therefore, total malloc will need PositionBufferTrackerCapacity*sizeof(MergeSoredKeyTracker)
    size_t positionBufferTracker;
    size_t currentPositionBufferTrackerCapacity; 
    
    MergeSortedKeyTracker *keys;
    PositionInExtensibleByteBuffer *kvaluesGroups;

    //to keep track of whether keys are ordered.
    bool orderingRequired;
    //to keep track of whether values are aggregated.
    bool aggregationRequired; 
    //to record whether it is activated.
    bool activated; 
   
    MergeSortedMapBuckets(int rId, bool ordering, bool aggregation):
      reducerId(rId),
      orderingRequired(ordering),
      aggregationRequired(aggregation),
      activated(false) {
       //NOTE: this is just the initial size. it will grow depending on actual computation
	if (orderingRequired || aggregationRequired ) {
          currentKeyTrackerCapacity =
	      SHMShuffleGlobalConstants::MERGESORTED_KEY_TRACKER_SIZE;
          currentPositionBufferTrackerCapacity =
    	      SHMShuffleGlobalConstants::MERGESORTED_POSITIONBUFFER_TRACKER_SIZE;
          //later, this will come from the pool.
          keys = (MergeSortedKeyTracker *)
	          malloc(currentKeyTrackerCapacity * sizeof (MergeSortedKeyTracker));
          kvaluesGroups = (PositionInExtensibleByteBuffer *)
	          malloc(currentPositionBufferTrackerCapacity* sizeof(PositionInExtensibleByteBuffer));
          activated = true;
       }
       else {
         currentKeyTrackerCapacity =0;
         currentPositionBufferTrackerCapacity = 0;
         keys = nullptr;
         kvaluesGroups = nullptr; 
       }

       //start with 0 
       keyTracker=0;
       positionBufferTracker =0;
   }

    //we need to invoke this check, before doing actual add key/value operations.
    bool isActivated () {
      return activated; 
    }

    //add a key, and return the key tracker position
    //NOTE:what is returned is the position that holds the added key, which is one less than the key tracker
    //after the key is inserted.
    size_t addKey(int kvalue){
        if (keyTracker == currentKeyTrackerCapacity) {
           LOG(INFO) << "keys " << (void*) keys << " with key tracker: " << keyTracker
	      << " reaches tracker capacity: " << currentKeyTrackerCapacity;
           //re-allocate then.
           currentKeyTrackerCapacity += SHMShuffleGlobalConstants::MERGESORTED_KEY_TRACKER_SIZE;
           keys = (MergeSortedKeyTracker*)realloc(keys, currentKeyTrackerCapacity*sizeof(MergeSortedKeyTracker));
        }

        keys[keyTracker].key=kvalue;
        //at this time, there is no value added. so start and end positions are the same.
        keys[keyTracker].start_position = positionBufferTracker;
        keys[keyTracker].end_position = positionBufferTracker;

        size_t currentIndex = keyTracker;
        keyTracker ++;
        return currentIndex;
   }
    
    //add a value at the specified key position
    void addValueOnKey (size_t keyPosition, const PositionInExtensibleByteBuffer &position){
          if (positionBufferTracker == currentPositionBufferTrackerCapacity ) {
              LOG(INFO) << "position tracker " << (void*)kvaluesGroups 
                        << " with position tracker: " << positionBufferTracker
	                << " reaches tracker capacity: " << currentPositionBufferTrackerCapacity;
              //re-allocate then.
              currentPositionBufferTrackerCapacity +=
                       SHMShuffleGlobalConstants::MERGESORTED_POSITIONBUFFER_TRACKER_SIZE;
              kvaluesGroups = (PositionInExtensibleByteBuffer*)realloc(kvaluesGroups,
  		        currentPositionBufferTrackerCapacity*sizeof(PositionInExtensibleByteBuffer));
          }

          kvaluesGroups[positionBufferTracker] = position;
          positionBufferTracker++;

          //update the end position, for the current key. the start position is already set when key is entered.
          keys[keyPosition].end_position = positionBufferTracker; 

          VLOG(3) << "In: addValue " << " key position: " << keyPosition 
                 << " corresponding start-position: " << keys[keyPosition].start_position
                 << " end-position: " << keys[keyPosition].end_position
		 << " current position bufffer tracker: " << positionBufferTracker <<endl;
	
    }


    //clear out all of the internal position pointers. 
    void reset() {
        //start with 0 
        keyTracker=0;
        positionBufferTracker =0;
    }

    //to free the held resources, and return them to the pool.
    void release() {
        //start with 0 
        keyTracker=0;
        positionBufferTracker =0;

        //free the resources. later, we will put them to the pooled resources.
        if (keys != nullptr ) {
           free((void*)keys);
           keys = nullptr;
	}
        
	if (kvaluesGroups != nullptr ) {
           free((void*)kvaluesGroups);
           kvaluesGroups = nullptr;
	}
    }
  };

};

#endif /*MERGE_SORT_KEY_POSITION_TRACKER_WITH_INT_KEYS_H_*/


