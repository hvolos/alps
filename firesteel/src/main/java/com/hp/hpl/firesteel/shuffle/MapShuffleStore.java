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

package com.hp.hpl.firesteel.shuffle;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

/**
 * Created by junli on 4/29/2015.
 * to allow the Map task to store the serialized data records from Java into the C++ shuffle
 * engine, along with the partitioner number assigned for each (k,v) pair
 */
public interface MapShuffleStore {

        /**
         * to initialize the storage space for a particular shuffle stage's map instance
         * @param shuffleId the current stage id
         * @param mapTaskId  the current map task Id
         * @param numberOfPartitions  the total number of the partitions chosen for the reducer.
         * @param keyType the type of the key to be created for the shuffle store in C++ shuffle engine
         * @param sizeOfBatchSerialization: the predefined size to conduct the batched serialization from Spark.
         * @param ordering, to specify whether the keys need to be ordered or not for map shuffle.
         * @return sessionId, which is the pointer to the native C++ shuffle store.
         */
         void initialize  (int shuffleId, int mapTaskId, int numberOfPartitions,
                                                ShuffleDataModel.KValueTypeId keyType,
                                                int sizeOfBatchSerialization,
                                                boolean ordering);

         /**
          * to stop the current map store
          */
         void stop();

        /**
         * to reclaim the resources required for shuffling this map task. This one
         * is done
         *
         * NOTE: who will issue this shutdown at what time
         *
         */
         void shutdown();

        /**
         * to serialize the (K,V) pairs  into the byte buffer, and record the offests in the byte
         * buffer for each K values and V values. Both K and V will be serialized
         *
         * @param kvalues the array of the K values
         * @param vvalues the array of the V values
         * @param numberOfPairs the size of the array of (K,V) pairs
         * the result is in koffests,  the array that records each offset of the K values serialized in the
         *                  byte buffer
         * the result is in voffsets, the array that records each offset of the V values serialized in the
         *                 byte byffer
         */
         void serializeKVPairs ( ArrayList<Object> kvalues, ArrayList<Object> vvalues, int numberOfPairs);

        /**
         * to store serialized (K,V) pairs stored in the byte buffer, into the C++ shared-memory
         * region
         * combined with private member: koffsets the offests for the K values
         * combined with private member: voffests the offsets for the V values
         * @param partitions the partition number computed by the partitioner function for each
         *                  (K,V) pair
         * @param numberOfPairs the size of the (K,V) pairs that have been serialized and to be
         */
         void storeKVPairs(ArrayList<Integer> partitions, int numberOfPairs);


        /**
         * to serialize the V values for the(K,V) pairs that have the K values to be with type of
         * int, float, long, string that are supported by  C++.
         * @param vvalues the V values
         * then store in private member: voffsets the array that records each offset of the V values 
         * serialized in the byte buffer
         * @param numberOfVs the size of the array of (K,V) pairs
         */
         void serializeVs (ArrayList<Object> vvalues, int numberOfVs);


         /**
          * Speical case: to store the (K,V) pairs that have the K values to be with type of float
          * combined with private member: voffsets
          * @param kvalues
          * @param partitions
          * @param numberOfPairs
          */
         void storeKVPairsWithIntKeys (ArrayList<Integer> kvalues,
        		                             ArrayList<Integer> partitions, int numberOfPairs);

        /**
         * Speical case: to store the (K,V) pairs that have the K values to be with type of float
         * combined with private member: voffsets
         * @param kvalues
         * @param partitions
         * @param numberOfPairs
         */
         void storeKVPairsWithFloatKeys (ArrayList<Float> kvalues, 
        		                            ArrayList<Integer> partitions, int numberOfPairs);


        /**
         * Special case: to store the (K,V) pairs that have the K values to be with type of long
         * combined with private member: voffsets
         * @param kvalues
         * @param partitions
         * @param numberOfPairs
         */
        void storeKVPairsWithLongKeys (ArrayList<Long> kvalues, 
        		                          ArrayList<Integer> partitions, int numberOfPairs);

        /**
         * Special case: To serialize the (K,V) pairs that have the K values to be with type of string
         * combined with private member: voffsets
         * @param kvalues
         * @param partitions
         * @param numberOfPairs
         */
        void storeKVPairsWithStringKeys (ArrayList<String> kvalues, 
        		                             ArrayList<Integer> partitions, int numberOfPairs);

         /**
          * to sort and store the sorted data into non-volatile memory that is ready for  the reduder
          * to fetch
          * @return status information that represents the map processing status
          */
         ShuffleDataModel.MapStatus sortAndStore();

        /**
        * to query what is the K value type used in this shuffle store
        * @return the K value type used for this shuffle operation.
        */
        ShuffleDataModel.KValueTypeId getKValueTypeId();

       /**
        * to set the K value type used in this shuffle store
        * @param ktype
        */
        void setKValueTypeId(ShuffleDataModel.KValueTypeId ktype);


         /**
          * based on a given particular V value, to store its value type.
          * @param Vvalue
          */
        void storeVValueType(Object Vvalue);

         /**
          * based on a given particular object based (K,V) pair, to store the corresponding types.
          * @param Kvalue
          * @param VValue
          */
        void storeKVTypes (Object Kvalue, Object VValue);

        /**
         * to support when K value type is an arbitrary object type. to retrieve the serialized
         * type information for K values that can be de-serialized by Java/Scala
        */
        byte[] getKValueType();

         /**
          * to retrieve the serialized type information for the V values that can be
          * de-serialized by Java/Scala
          */
        byte[] getVValueType();
        
        /**
         * retrieve the unique store id. 
         * @return
         */
        int getStoreId(); 

}
