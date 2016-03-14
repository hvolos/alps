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

#include "SimpleUtils.h"
#include "MergeSortReduceChannelWithStringKeys.h"
#include "ShuffleDataSharedMemoryManager.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "MapShuffleStoreManager.h"
#include <cassert> 
#include <iostream>


inline bool ComparatorForPriorityQueuedElementWithVariableLengthKey::operator()(
       const PriorityQueuedElementWithVariableLengthKey &a,
       const PriorityQueuedElementWithVariableLengthKey &b){

  //first compare the normalized key
  if (a.normalizedKey > b.normalizedKey) {
    return true;
  }
  else if (a.normalizedKey < b.normalizedKey) {
    return false; 
  }
  else if (a.normalizedKey == b.normalizedKey) {
     //then we need to do the full comparision.
     //a.fullKeyValue, b.fullKeyValue
    bool comp_result=false;

    //get to the actual key, we advance char by char, until it is done.
    bool done=false;

    //Note: the buffer used is actually non-extensible buffer.
    int a_position=a.fullKeyValue.position_in_start_buffer;
    int b_position=b.fullKeyValue.position_in_start_buffer;
    int scanned=0;
    while (!done) {
      unsigned char va = kvBufferMgr->current_buffer().value_at(a_position);
      unsigned char vb = kvBufferMgr->current_buffer().value_at(b_position);
      scanned++;

      //we get the winner.                                                                                                                         
      if (va > vb) {
	done = true;
	comp_result=true;
      }
      else if (va < vb) {
	done = true;
	comp_result=false;
      }
      else {
	//need to advance to next character.                                                                                                       
	//one of them may be exhausted, the other still have more                                                                                         
	//both can advance to next char
        if (scanned == a.fullKeyValue.value_size) {
	    //a exhausted. 
            if (scanned < b.fullKeyValue.value_size) {
               comp_result = false;
	    }
            else if (scanned == b.fullKeyValue.value_size) {
               comp_result=false; //they are fully identical, pick one
	    }
	 
            done = true;
            break;
        }
       
        if (scanned  == b.fullKeyValue.value_size) {
           //b exhaused.
	   if (scanned < a.fullKeyValue.value_size) {
	     comp_result = true;
  	   }
           else if (scanned == a.fullKeyValue.value_size) {
              comp_result = false; //they are fully identical, pick one
    	   }

           done = true;
           break;
        }

        a_position++;
        b_position++;
      } //end else
    }//end done

    return  comp_result;
  }//normalized key

  return false; //to make compiler happy for no warning.
}


//the implementation is the same as the int key
void MergeSortReduceChannelWithStringKeys::init() {
   CHECK_NE(mapBucket.mapId, -1);

   RRegion::TPtr<void> global_null_ptr;

   uint64_t gregion_id = mapBucket.regionId;
   uint64_t goffset = mapBucket.offset;

   unsigned char *indexchunk_offset = nullptr;
   if (SHMShuffleGlobalConstants::USING_RMB){
     RRegion::TPtr<void> globalpointer(gregion_id, goffset);
     if (globalpointer != global_null_ptr) {
      indexchunk_offset = (unsigned char*) globalpointer.get();
     }
     //else it is still nullptr;
   }
   else {
     //local memory allocator
     indexchunk_offset = reinterpret_cast<unsigned char *>(goffset);
   }
  
   CHECK (indexchunk_offset != nullptr); //

   unsigned char *p = indexchunk_offset;

   VLOG(2) << "retrieved index chunk offset is: " << (void*)indexchunk_offset;
   int keytypeId = ShuffleDataSharedMemoryReader::read_indexchunk_keytype_id(p);
   VLOG(2) << "retrieved index chunk key type id is: " << keytypeId;

   p += sizeof(keytypeId);

   int kclass_size = 0;
   int vclass_size = 0;
   //so far, we just skipped reading key Value and Value Value.
   //unsigned char *kvalue=nullptr;
   //unsigned char *vvalue=nullptr;

  if ((keytypeId == KValueTypeId::Int)
	|| (keytypeId == KValueTypeId::Long)
	|| (keytypeId == KValueTypeId::Float)
	|| (keytypeId == KValueTypeId::Double)
	|| (keytypeId == KValueTypeId::String)){

	vclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
	VLOG(2) << "retrieved index chunk value class size: " << vclass_size;

	p += sizeof(vclass_size); 
        //advance to value's first byte.
        //skipp to read actual Key'value and Value's value. the actual vclass is fetched 
        //from mergesort channel helper, when reduce shuffle is created.
	p += vclass_size;
  }
  else if (keytypeId == KValueTypeId::Object) {
	kclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_keyclass(p);
	p += sizeof(kclass_size);
	vclass_size = ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
	p += sizeof(vclass_size);

	//I am now skipp to read actual Key'value and Value's value. just advance the pointer                   
	p += kclass_size;
	p += vclass_size;
  }

  int buckets_size = 0;
  unsigned char *toLogPicBucketSize = p;
  ShuffleDataSharedMemoryReader::read_indexchunk_number_total_buckets(p, &buckets_size);
  VLOG(2) << "retrieved index chunk bucket size: " << buckets_size
	  << " at memory address: " << (void*)toLogPicBucketSize;

  p += sizeof(buckets_size);

  //note: we may do range checking here.
  CHECK_EQ(buckets_size, totalNumberOfPartitions);

  //then we will move to the position specified by the map id, which starts from 0, to 
  //the number of partitions
  uint64_t datachunk_gregion_id = -1;
  uint64_t datachunk_goffset = -1;

  int  bsize = 0;
  //NOTE: it is the reduce id that I need to retrieve from this map bucket, note map id.
  p += reducerId *(sizeof(datachunk_gregion_id)+ sizeof(datachunk_goffset) + sizeof(bsize));

  unsigned char *toLogPicOffset = p;
  ShuffleDataSharedMemoryReader::read_indexchunk_bucket(p,
					&datachunk_gregion_id, &datachunk_goffset, &bsize);

  VLOG(2) << "retrieved index chunk bucket for map id: " << mapBucket.mapId
	  << " with data chunk region id: " << (void*)datachunk_gregion_id
	  << " with data chunk offset: " << (void*)datachunk_goffset
	  << " and size: " << bsize
	  << " at memory address: " << (void*)toLogPicOffset;

  //NOTE: we also need to update the total length of the map bucket. as what gets passed
  //from  the Spark scheduler to the reducer, is just an approximation.
  totalLength = bsize;
  if (SHMShuffleGlobalConstants::USING_RMB) {
    RRegion::TPtr<void> globalpointer(datachunk_gregion_id, datachunk_goffset);
    if (globalpointer != global_null_ptr) {
     currentPtr =(unsigned char*) globalpointer.get();
    }

  }
  else {
    //local memory allocator
    currentPtr = reinterpret_cast<unsigned char*>(datachunk_goffset);
  }

}


void MergeSortReduceChannelWithStringKeys::getNextKeyValuePair() {
	unsigned char *oldPtr = currentPtr; 

        //(1) read key's size, then advance the pointer;
	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr, 
					    (unsigned char*) &currentKeySize, sizeof(currentKeySize));

	VLOG(2) << "retrieved data chunk for map id: " << mapBucket.mapId
		<< " with key value's size: " << currentKeySize;
	currentPtr += sizeof(currentKeySize);

        //(2) read the key value out, then advance the pointer;
        currentKeyValue =ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
			                  currentPtr, currentKeySize, bufferMgr); 
	currentPtr += currentKeySize;

	VLOG(2) << "retrieved data chunk key's value: " << " start buffer: " << currentKeyValue.start_buffer 
            << " position: " << currentKeyValue.position_in_start_buffer
    	    << " size: " << currentKeyValue.value_size 
            << " with buffer manager internal position: " << bufferMgr->current_buffer().position_in_buffer();

        //(3) read value's size, then advance the pointer
	ShuffleDataSharedMemoryReader::read_datachunk_value(currentPtr,
					    (unsigned char*)&currentValueSize, sizeof(currentValueSize));
        currentPtr += sizeof(currentValueSize); 
        //(4) read value's value, then advance the pointer.
	currentValueValue =
		ShuffleDataSharedMemoryReader::read_datachunk_keyassociated_value(
                  currentPtr, currentValueSize, bufferMgr); 

	VLOG(2) << "retrieved data chunk value: " << " start buffer: " << currentValueValue.start_buffer 
                << " position: " << currentValueValue.position_in_start_buffer
		<< " size: " << currentValueValue.value_size 
                << " with buffer manager internal position: " << bufferMgr->current_buffer().position_in_buffer();

	//after that , you need to move the pointer
	currentPtr += currentValueSize;

	kvalueCursor++;
	totalBytesScanned += (currentPtr - oldPtr); 
}


int MergeSortReduceChannelWithStringKeys::retrieveKeyWithMultipleValues(
	                    vector <PositionInExtensibleByteBuffer> & heldValues, vector <int> & heldValueSizes) {
	int count = 0; 
	PositionInExtensibleByteBuffer keyToCompared = currentKeyValue; 
	heldValues.push_back(currentValueValue);
	heldValueSizes.push_back(currentValueSize);
	count++; 

	while (hasNext()) {
		getNextKeyValuePair();
		PositionInExtensibleByteBuffer keyValue = currentKeyValue;
		if (VariableLengthKeyComparator::areEqual(keyValue,keyToCompared, bufferMgr) ){
			//currentValueValue and currentValueSize already get changed due to 
			heldValues.push_back(currentValueValue);
			heldValueSizes.push_back(currentValueSize);
			count++; 
		}
		else {
			break; //done, but the current key alreayd advances to the next different key.  
		}
	}

	return count; 
}


void MergeSortReduceEngineWithStringKeys::init() {
	int channelNumber = 0; 
	for (auto p = mergeSortReduceChannels.begin(); p != mergeSortReduceChannels.end(); ++p) {
		p->init();
		//check, to make sure that we have at least one element for each channel. 
		//for testing purpose, p may have zero elements inside. 
		if (p->hasNext()) {
			//populate the first element from each channel into the priority.
			p->getNextKeyValuePair();
			PositionInExtensibleByteBuffer firstValue = p->getCurrentKeyValue();
			PriorityQueuedElementWithVariableLengthKey
                                                   firstElement(channelNumber, firstValue, bufferMgr);
			mergeSortPriorityQueue.push(firstElement);
		}

		//still, we are using unique numbers. 
		channelNumber++; 
	}

	//NOTE: should the total number of the channels to be merged is equivalent to total number of partitions?
	//or we only consider the non-zero-sized buckets/channels to be merged? 

}

/*
 *for the top() element, find out which channel it belongs to, then after the channel to advance to 
 *fill the elements that has the same value as the current top element, then fill the vacant by pushing
 * into the next key value, if it exists. 
 * we return, until the top() return is different from the current value
 */
void MergeSortReduceEngineWithStringKeys::getNextKeyValuesPair() {
   //clean up the value and value size holder. the values held in the vector will have the occupied memory
   //freed in some other places and other time.
   currentMergedValues.clear();
   currentMergeValueSizes.clear();

   PriorityQueuedElementWithVariableLengthKey topElement = mergeSortPriorityQueue.top();
   int channelNumber = topElement.mergeChannelNumber;
   currentMergedKey = topElement.fullKeyValue;

   mergeSortPriorityQueue.pop();

   MergeSortReduceChannelWithStringKeys &channel = mergeSortReduceChannels[channelNumber];
	
   //for this channel, to advance to the next key that is different from the current one;
   channel.retrieveKeyWithMultipleValues(currentMergedValues, currentMergeValueSizes);
   //this is after the duplicated keys for this channel. 

   PositionInExtensibleByteBuffer  nextKeyValue = channel.getCurrentKeyValue();
   if (! VariableLengthKeyComparator::areEqual(nextKeyValue, currentMergedKey, bufferMgr) ){
	//because we advance from the last retrieved duplicated key
       PriorityQueuedElementWithVariableLengthKey 
                       replacementElement(channelNumber, nextKeyValue, bufferMgr);
       mergeSortPriorityQueue.push(replacementElement);
   }
   //else: the channel is exhaused. 

   //keep moving to the other channels, until current merged key is different
 
   while (!mergeSortPriorityQueue.empty()) {
	PriorityQueuedElementWithVariableLengthKey nextTopElement = mergeSortPriorityQueue.top();
	PositionInExtensibleByteBuffer nextKeyValue = nextTopElement.fullKeyValue;
	
	int other_channelnumber = nextTopElement.mergeChannelNumber;

	//this is a duplicated key, from the other channel. we will do the merge.
	if (VariableLengthKeyComparator::areEqual(nextKeyValue,currentMergedKey,bufferMgr))  {
	  CHECK_NE(other_channelnumber, channelNumber);
   	 
          mergeSortPriorityQueue.pop();
			 
	  MergeSortReduceChannelWithStringKeys &other_channel = mergeSortReduceChannels[other_channelnumber];

	  //for this channel, to advance to the next key that is different from the current one;
	  other_channel.retrieveKeyWithMultipleValues(currentMergedValues, currentMergeValueSizes);
	  //pick the next one from this other channel, or we already exhausted. 
	  PositionInExtensibleByteBuffer other_nextKeyValue = other_channel.getCurrentKeyValue();
	  if (!VariableLengthKeyComparator::areEqual(other_nextKeyValue ,currentMergedKey, bufferMgr)){
		PriorityQueuedElementWithVariableLengthKey 
		  other_replacementElement(other_channelnumber, other_nextKeyValue, bufferMgr);
		// the other_channel gets popped, so we need to push the corresponding replacement element,
                // if it is available. 
		mergeSortPriorityQueue.push(other_replacementElement);
	  }
	  //else, this channel is also exhausted. 
	}
	else {
	   //the top of the queue has different key value different from the current one, 
           //that is no more duplicated queue. 
	   break; 
	}
   }
	
}
