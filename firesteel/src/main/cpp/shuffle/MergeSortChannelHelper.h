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

#ifndef MERGE_SORT_CHANNEL_HELPER_H_
#define MERGE_SORT_CHANNEL_HELPER_H_

#include "MapStatus.h"
#include "ExtensibleByteBuffers.h"
#include "EnumKvTypes.h"

class MergeSortChannelHelper {
 public: 

  //to obtain the index chunk and then retrieve the Key's type definition
  //and Value type definition.
  static void obtain_kv_definition (MapBucket &mapBucket, int rId, int rPartitions,
       			             KValueTypeDefinition &kd, VValueTypeDefinition &vd);

};



#endif /*MERGE_SORT_CHANNEL_HELPER_H_*/
