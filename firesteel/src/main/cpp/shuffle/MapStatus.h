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

#ifndef MAPSTATUS_H_
#define MAPSTATUS_H_

#include <glog/logging.h>
#include <cassert>
#include <string> 
#include <vector>

using namespace std; 

//a map buckt is represented by its size, the corresponding shared-memory region
//and the offset in the specified shared-memory region
struct MapBucket {
  int partitionId; //match the reducer id.
  int size; 
  //string shmRegionName;
  uint64_t regionId;
  uint64_t offset;
  int mapId;

  MapBucket(int pid, int s, uint64_t regId, long offt, int mId):
     partitionId (pid), size(s), regionId(regId), offset(offt), mapId(mId) {
  }
};

//a Reduce Status is represented by its reducer id, and the set of the map buckets
//each of which has the partition id matched with the reduce id.
struct ReduceStatus {
  int reduceId;
  vector<MapBucket> mapBuckets;
  int sizeOfMapBuckets; 

  ReduceStatus (int rid) : reduceId(rid), sizeOfMapBuckets(0) {
    //
  }

  void addMapBucket(const MapBucket &bucket) {
    mapBuckets.push_back(bucket);
  }

  //vector::at(.) comes with range checking.
  //NOTE: can we pass a reference instead of copy the results, while at the same
  //time to take care that a dummy result is returned while no results are found? 
  MapBucket bucket_for_mapid(int mapId) {
	  //WARNING: this is a full scan. We need to do the scan of the entire map buckets
          //to get the matched map id.
	  MapBucket result(0, 0, -1, -1, -1);   
	 
	  bool found = false; 
	  for (auto p = mapBuckets.begin(); p != mapBuckets.end(); ++p) {
		  if (p->mapId == mapId)  {
			  result = *p;
			  found = true; 
		  }
	  }

	  CHECK_EQ(found, true);
	  return result; 
  }

  int getSizeOfMapBuckets() {
	  return mapBuckets.size();
  }
};

struct MapStatus {
  int mapId;  //we need map id to differentiate different mapper. 
  //string shmRegionName;
  uint64_t  regionIdOfIndexBucket; //representing the region id of the index bucket
  uint64_t  offsetOfIndexBucket; //representing the offset of the index bucket
  //each bucket will have its size, which can be 0.
  vector <int> bucketSizes;
  int totalNumberOfPartitions; 

  MapStatus(uint64_t regionId, uint64_t offset, int nPartitions, int mId):
	  mapId(mId),
      regionIdOfIndexBucket(regionId), offsetOfIndexBucket(offset),
      totalNumberOfPartitions(nPartitions)

  {
    //initialize the map status vector 
    for (int i=0; i<totalNumberOfPartitions; i++) {
      bucketSizes.push_back(0);
    }
  }

  void setBucketSize(int partition, int size){
     //element zero to be the first one.
    bucketSizes[partition]=size;
  }

  int getMapId() {
     return mapId;
  }
  
  uint64_t getRegionId() {
     return regionIdOfIndexBucket;
  }

  uint64_t getOffsetOfIndexBucket() {
    return offsetOfIndexBucket;
  }

  vector<int> & getBucketSizes() {
    return bucketSizes;
  }
};

#endif 
