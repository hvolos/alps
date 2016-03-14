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

#ifndef HASH_MAP_KEY_POSITION_TRACKER_H_
#define HASH_MAP_KEY_POSITION_TRACKER_H_

#include "ShuffleConstants.h"
#include "ExtensibleByteBuffers.h"
#include <stdlib.h>

//to define the structure that only is with fixed length key.
//NOTE: this is general to all of the different types of the keys.
namespace KeyWithFixedLength {

  struct HashMapValueLinkingTracker {
     PositionInExtensibleByteBuffer  value;
     int previous_element;  //create the backward linking list.
  };

  /*
   * this is designed for hash-map based merging of the values with the same key value
   * this is to be re-used across different reduce shuffle task, via pooling.
   */
  struct HashMapValueLinkingWithSameKey {
    int reducerId; 
    //the curent allocated number of elements to hold the key tracker.
    //therefore, total malloc will need valueTrackerCapacity*sizeof(HashMapValueLinkingTracker)
    size_t valueTracker;
    size_t currentValueTrackerCapacity; 
    
    HashMapValueLinkingTracker *valuesBeingTracked;

    HashMapValueLinkingWithSameKey (int rId);

    ~HashMapValueLinkingWithSameKey() {
      //do nothing.
    }


    //add a key, and return the key tracker position
    //NOTE:what is returned is the position that holds the added key, which is one less than the key tracker
    //after the key is inserted.
    size_t addValue(const PositionInExtensibleByteBuffer &value_position);

    //add a value at the specified key position.
    void addLinkingOnValue (int currentValuePosition, int pointer_to_element);

    //clear out all of the internal position pointers. 
    //NOTE: this method will be called when pooling is turned on.
    void reset();

    //to free the held resources, and return them to the pool.
    void release();

  };

};

#endif /*HASH_MAP_KEY_POSITION_TRACKER_H_*/


