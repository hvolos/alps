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

#include "AttributeHashTable.h"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <sys/time.h>

AttributeHashTable::AttributeHashTable() {
}

AttributeHashTable::~AttributeHashTable() {
}

// create two long tables for hash index
void AttributeHashTable::create2tables(int regionId) {

	// memory allocation for the first int array 
	bucket_cnt = hashTable.bucket_count();
	arr1stTable = (int*)malloc(bucket_cnt*sizeof(int));

	unordered_map < long, long >::hasher fn = hashTable.hash_function();

	// hash function
	size_t pos = 0;
	vector<long> temp_vec;
	// populate the temporary vector for the second long array
	for (unsigned i = 0; i < hashTable.bucket_count(); ++i) {

		arr1stTable[i] = -1;

		for ( auto x = hashTable.begin(i); x!= hashTable.end(i); ++x ) {
			temp_vec.push_back(x->first); // key

			if(arr1stTable[i] == -1) {
				arr1stTable[i] = pos; // the position to the second array where the first bucket value is located
			}
			pos++;
			temp_vec.push_back(x->second); // value
			pos++;
       	}
		temp_vec.push_back(-1);
		pos++;
	}	
	// memory allocation for the second long array 
	arr2ndSize = temp_vec.size();
	arr2ndTable = (long*)malloc(arr2ndSize*sizeof(long));

	// memory copy from temporary vector to the second long array 
	memcpy(arr2ndTable, temp_vec.data(), temp_vec.size()*sizeof(long));
	
	hashTable.clear(); // clean up the unordered_map

	
}

