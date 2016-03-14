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

#include <glog/logging.h>
#include "HashMapKeyPositionTracker.h"
#include "ArrayBufferPool.h"
#include "ShuffleStoreManager.h"

KeyWithFixedLength::HashMapValueLinkingWithSameKey::HashMapValueLinkingWithSameKey (int rId):
                 reducerId(rId){
       //NOTE: this is just the initial size. it will grow depending on actual computation
       ArrayBufferElement bElement = 
                  ShuffleStoreManager::getInstance()->getLinkedValueBufferPool()->getBuffer();
       if (bElement.capacity == 0) {
           currentValueTrackerCapacity = SHMShuffleGlobalConstants::HASHMAP_VALUE_TRACKER_SIZE;
           valuesBeingTracked = (HashMapValueLinkingTracker *)
	          malloc(currentValueTrackerCapacity * sizeof (HashMapValueLinkingTracker));
       }
       else {
           currentValueTrackerCapacity = bElement.capacity;
	   valuesBeingTracked = (HashMapValueLinkingTracker *)bElement.start_address;
       }

       //start with 0 
       valueTracker=0;
}

size_t KeyWithFixedLength::HashMapValueLinkingWithSameKey::addValue(
                     const PositionInExtensibleByteBuffer &value_position){
       if (valueTracker == currentValueTrackerCapacity) {
           LOG(INFO) << "values " << (void*) valuesBeingTracked << " with value tracker: " << valueTracker
		     << " reaches tracker capacity: " << currentValueTrackerCapacity;
           //extend the tracker capacity, and re-allocate the buffer
           currentValueTrackerCapacity += SHMShuffleGlobalConstants::HASHMAP_VALUE_TRACKER_SIZE;
           valuesBeingTracked = 
               (HashMapValueLinkingTracker* )realloc(valuesBeingTracked, 
                currentValueTrackerCapacity*sizeof(HashMapValueLinkingTracker));
           LOG(INFO) << "values being re-allocated to: " << (void*) valuesBeingTracked << " with value tracker: " << valueTracker
		     << " and new tracker capacity: " << currentValueTrackerCapacity;
       }

       valuesBeingTracked[valueTracker].value=value_position;
       //if we put with 0, then the first element with 0 index may be linked from others, and we can not
       //differentiate this two postion
       valuesBeingTracked[valueTracker].previous_element= -1; //no linking at this time.

       size_t currentIndex = valueTracker;
       valueTracker ++;
       return currentIndex;
}
    
//add a value at the specified key position.
void KeyWithFixedLength::HashMapValueLinkingWithSameKey::addLinkingOnValue (
                                  int currentValuePosition, int pointer_to_element){
       valuesBeingTracked[currentValuePosition].previous_element =pointer_to_element;
 
       VLOG(3) << "In: addLinkingOnValue " << " value position: " << currentValuePosition
		<< " pointing to value position: " << pointer_to_element;
}


//clear out all of the internal position pointers. 
//NOTE: this method will be called when pooling is turned on.
void KeyWithFixedLength::HashMapValueLinkingWithSameKey::reset() {
       //start with 0 
       valueTracker=0;
}

//to free the held resources, and return them to the pool.
void KeyWithFixedLength::HashMapValueLinkingWithSameKey::release() {
       //start with 0 
       valueTracker=0;
       //NOTE: free the resources. later, we will put them to the pooled resources.
       ArrayBufferElement element (currentValueTrackerCapacity, (unsigned char*)valuesBeingTracked);
       //free((void*)valuesBeingTracked);
       ShuffleStoreManager::getInstance()->getLinkedValueBufferPool()->freeBuffer(element);
       valuesBeingTracked = nullptr;
}

