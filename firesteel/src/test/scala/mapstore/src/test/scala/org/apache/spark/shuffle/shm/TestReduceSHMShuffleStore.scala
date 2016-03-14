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

package org.apache.spark.shuffle.shm


import java.nio.ByteBuffer

import org.apache.spark.LocalSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.FunSuite
import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf, Logging}
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer._
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList

/**
 * Created by junli on 7/24/2015.
 */

class TestReduceSHMShuffleStore extends FunSuite with LocalSparkContext with Logging {

  private def getThreadLocalShuffleResource(conf: SparkConf):
  ThreadLocalShuffleResourceHolder.ShuffleResource = {
    val SERIALIZATION_BUFFER_SIZE: Int =
      conf.getInt("spark.shuffle.shm.serializer.buffer.max.mb", 64) * 1024 * 1024;
    val resourceHolder = new ThreadLocalShuffleResourceHolder()
    var shuffleResource = resourceHolder.getResource()
    if (shuffleResource == null) {
      val kryoInstance = new KryoSerializer(SparkEnv.get.conf).newKryo();
      //per-thread
      val serializationBuffer = ByteBuffer.allocateDirect(SERIALIZATION_BUFFER_SIZE)
      resourceHolder.initilaze(kryoInstance, serializationBuffer)
      shuffleResource = resourceHolder.getResource()
    }
    shuffleResource
  }

  ignore ("reduceId 0 with class registration") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize()
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int


    val threadLocalResources = getThreadLocalShuffleResource(conf)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType)

    val reduceId = 0
    val reduceSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
                      threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer,
        shuffleId, reduceId, numberOfPartitions)

    var mapStatusResult = null.asInstanceOf[ShuffleDataModel.MapStatus]

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()

    for (i <- 0 to numberOfVs-1 ) {
      val obj = new RankingsClass(i, "hello" +i, i+1)
      testObjects.add(obj)
      voffsets.add(0)
      partitions.add( i%2 )
      kvalues.add (i)
    }

      //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
      mapSHMShuffleStore.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
        voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
      //before storeKVpairs, the Value Type needs to be stored already.
      mapSHMShuffleStore.storeVValueType(testObjects.get(0))
      // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
      //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
      val numberOfPairs = numberOfVs
      mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
        kvalues.asInstanceOf[ArrayList[Integer]],
        partitions.asInstanceOf[ArrayList[Integer]],
        numberOfPairs)

      mapStatusResult = mapSHMShuffleStore.sortAndStore()

      logInfo("map status region name: " + mapStatusResult.getShmRegionName())
      logInfo ("map status offset to index chunk: 0x "
        + java.lang.Long.toHexString(mapStatusResult.getOffsetToIndexBucket()))
      val buckets = mapStatusResult.getMapStatus()

      if (buckets != null) {
        for (i <- 0 to buckets.length-1) {
          logInfo ("map status, bucket: " + i + " has size: " + buckets(i))
        }
      }
      else {
        logInfo("map status buckets length is null.")
      }

    //reduce side:
    reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions)
    val mapIds = Seq (mapId).toArray
    val shmRegionNames = Seq (mapStatusResult.getShmRegionName()).toArray
    val offsetToIndexChunks = Seq (mapStatusResult.getOffsetToIndexBucket()).toArray
    val sizes = Seq (mapStatusResult.getMapStatus()(reduceId)).toArray //pick the first bucket

    val statuses =
      new ShuffleDataModel.ReduceStatus(mapIds, shmRegionNames, offsetToIndexChunks, sizes)
    //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet.
    reduceSHMShuffleStore.mergeSort(statuses)

    //retrieve the data
    val retrieved_knumbers = 6
    val retrieved_kvalues = new ArrayList[Integer]()
    val retrieved_vvalues = new ArrayList[ArrayList[Object]] ()
    for (i <- 0 to retrieved_knumbers-1) {
      retrieved_kvalues.add(0); //initialization to 0;
      retrieved_vvalues.add(null); //initialization to null;
    }

    val actualRetrievedKNumbers=
               reduceSHMShuffleStore.getKVPairsWithIntKeys (retrieved_kvalues,
                          retrieved_vvalues,
                          retrieved_knumbers)

    logInfo("actual number of the keys retrieved is: " + actualRetrievedKNumbers)

    for (i <-0 to actualRetrievedKNumbers-1) {
      logInfo("retrieved k value: " + retrieved_kvalues.get(i))
      val tvvalues = retrieved_vvalues.get(i)
      for (m <-0 to tvvalues.size()-1) {
        val x = tvvalues.get(m);
        assert (x.isInstanceOf[RankingsClass])
        if ( x.isInstanceOf[RankingsClass]) {

          val y =  x.asInstanceOf[RankingsClass]
          logInfo("**" + " object: " + " page rank:" + y.pagerank
            + " url: " + y.pageurl
            + " avg duration: " + y.avgduration)

          //based on how I constructed the test data
          assert (y.pagerank==retrieved_kvalues.get(i).intValue())
          assert (y.pageurl== "hello"+ retrieved_kvalues.get(i).intValue())
          assert (y.avgduration ==  retrieved_kvalues.get(i).intValue() + 1)
        }
      }
    }

    assert(actualRetrievedKNumbers == 5)

    val actuals  = new ArrayBuffer[Int]()
    for (i <- 0 to actualRetrievedKNumbers-1) {
      actuals += retrieved_kvalues.get(i)
    }

    val expecteds = Seq (0, 2, 4, 6, 8)
    assert  (expecteds === actuals.toArray)

    reduceSHMShuffleStore.stop()
    reduceSHMShuffleStore.shutdown()
    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

  ignore ("reduceId 0 without class registration") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    //conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize()
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int


    val threadLocalResources = getThreadLocalShuffleResource(conf)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType)

    val reduceId = 0
    val reduceSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer,
        shuffleId, reduceId, numberOfPartitions)

    var mapStatusResult = null.asInstanceOf[ShuffleDataModel.MapStatus]

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()

    for (i <- 0 to numberOfVs-1 ) {
      val obj = new RankingsClass(i, "hello" +i, i+1)
      testObjects.add(obj)
      voffsets.add(0)
      partitions.add( i%2 )
      kvalues.add (i)
    }

    //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
    mapSHMShuffleStore.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
      voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
    //before storeKVpairs, the Value Type needs to be stored already.
    mapSHMShuffleStore.storeVValueType(testObjects.get(0))
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs = numberOfVs
    mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
      kvalues.asInstanceOf[ArrayList[Integer]],
      partitions.asInstanceOf[ArrayList[Integer]],
      numberOfPairs)

    mapStatusResult = mapSHMShuffleStore.sortAndStore()

    logInfo("map status region name: " + mapStatusResult.getShmRegionName())
    logInfo ("map status offset to index chunk: 0x "
      + java.lang.Long.toHexString(mapStatusResult.getOffsetToIndexBucket()))
    val buckets = mapStatusResult.getMapStatus()

    if (buckets != null) {
      for (i <- 0 to buckets.length-1) {
        logInfo ("map status, bucket: " + i + " has size: " + buckets(i))
      }
    }
    else {
      logInfo("map status buckets length is null.")
    }

    //reduce side:
    reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions)
    val mapIds = Seq (mapId).toArray
    val shmRegionNames = Seq (mapStatusResult.getShmRegionName()).toArray
    val offsetToIndexChunks = Seq (mapStatusResult.getOffsetToIndexBucket()).toArray
    val sizes = Seq (mapStatusResult.getMapStatus()(reduceId)).toArray //pick the first bucket

    val statuses =
      new ShuffleDataModel.ReduceStatus(mapIds, shmRegionNames, offsetToIndexChunks, sizes)
    //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet.
    reduceSHMShuffleStore.mergeSort(statuses)

    //retrieve the data
    val retrieved_knumbers = 6
    val retrieved_kvalues = new ArrayList[Integer]()
    val retrieved_vvalues = new ArrayList[ArrayList[Object]] ()
    for (i <- 0 to retrieved_knumbers-1) {
      retrieved_kvalues.add(0); //initialization to 0;
      retrieved_vvalues.add(null); //initialization to null;
    }

    val actualRetrievedKNumbers=
      reduceSHMShuffleStore.getKVPairsWithIntKeys (retrieved_kvalues,
        retrieved_vvalues,
        retrieved_knumbers)

    logInfo("actual number of the keys retrieved is: " + actualRetrievedKNumbers)

    for (i <-0 to actualRetrievedKNumbers-1) {
      logInfo("retrieved k value: " + retrieved_kvalues.get(i))
      val tvvalues = retrieved_vvalues.get(i)
      for (m <-0 to tvvalues.size()-1) {
        val x = tvvalues.get(m);
        assert (x.isInstanceOf[RankingsClass])
        if ( x.isInstanceOf[RankingsClass]) {

          val y =  x.asInstanceOf[RankingsClass]
          logInfo("**" + " object: " + " page rank:" + y.pagerank
            + " url: " + y.pageurl
            + " avg duration: " + y.avgduration)

          //based on how I constructed the test data
          assert (y.pagerank==retrieved_kvalues.get(i).intValue())
          assert (y.pageurl== "hello"+ retrieved_kvalues.get(i).intValue())
          assert (y.avgduration ==  retrieved_kvalues.get(i).intValue() + 1)
        }
      }
    }

    assert(actualRetrievedKNumbers == 5)

    val actuals  = new ArrayBuffer[Int]()
    for (i <- 0 to actualRetrievedKNumbers-1) {
      actuals += retrieved_kvalues.get(i)
    }

    val expecteds = Seq (0, 2, 4, 6, 8)
    assert  (expecteds === actuals.toArray)

    reduceSHMShuffleStore.stop()
    reduceSHMShuffleStore.shutdown()
    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

  ignore ("reduceId 1 with class registration") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize()
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int


    val threadLocalResources = getThreadLocalShuffleResource(conf)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType)

    val reduceId = 1
    val reduceSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer,
        shuffleId, reduceId, numberOfPartitions)

    var mapStatusResult = null.asInstanceOf[ShuffleDataModel.MapStatus]

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()

    for (i <- 0 to numberOfVs-1 ) {
      val obj = new RankingsClass(i, "hello" +i, i+1)
      testObjects.add(obj)
      voffsets.add(0)
      partitions.add( i%2 )
      kvalues.add (i)
    }

    //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
    mapSHMShuffleStore.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
      voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
    //before storeKVpairs, the Value Type needs to be stored already.
    mapSHMShuffleStore.storeVValueType(testObjects.get(0))
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs = numberOfVs
    mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
      kvalues.asInstanceOf[ArrayList[Integer]],
      partitions.asInstanceOf[ArrayList[Integer]],
      numberOfPairs)

    mapStatusResult = mapSHMShuffleStore.sortAndStore()

    logInfo("map status region name: " + mapStatusResult.getShmRegionName())
    logInfo ("map status offset to index chunk: 0x "
      + java.lang.Long.toHexString(mapStatusResult.getOffsetToIndexBucket()))
    val buckets = mapStatusResult.getMapStatus()

    if (buckets != null) {
      for (i <- 0 to buckets.length-1) {
        logInfo ("map status, bucket: " + i + " has size: " + buckets(i))
      }
    }
    else {
      logInfo("map status buckets length is null.")
    }

    //reduce side:
    reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions)
    val mapIds = Seq (mapId).toArray
    val shmRegionNames = Seq (mapStatusResult.getShmRegionName()).toArray
    val offsetToIndexChunks = Seq (mapStatusResult.getOffsetToIndexBucket()).toArray
    val sizes = Seq (mapStatusResult.getMapStatus()(reduceId)).toArray //pick the first bucket

    val statuses =
      new ShuffleDataModel.ReduceStatus(mapIds, shmRegionNames, offsetToIndexChunks, sizes)
    //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet.
    reduceSHMShuffleStore.mergeSort(statuses)

    //retrieve the data
    val retrieved_knumbers = 6
    val retrieved_kvalues = new ArrayList[Integer]()
    val retrieved_vvalues = new ArrayList[ArrayList[Object]] ()
    for (i <- 0 to retrieved_knumbers-1) {
      retrieved_kvalues.add(0); //initialization to 0;
      retrieved_vvalues.add(null); //initialization to null;
    }

    val actualRetrievedKNumbers=
      reduceSHMShuffleStore.getKVPairsWithIntKeys (retrieved_kvalues,
        retrieved_vvalues,
        retrieved_knumbers)

    logInfo("actual number of the keys retrieved is: " + actualRetrievedKNumbers)

    for (i <-0 to actualRetrievedKNumbers-1) {
      logInfo("retrieved k value: " + retrieved_kvalues.get(i))
      val tvvalues = retrieved_vvalues.get(i)
      for (m <-0 to tvvalues.size()-1) {
        val x = tvvalues.get(m);
        assert (x.isInstanceOf[RankingsClass])
        if ( x.isInstanceOf[RankingsClass]) {

          val y =  x.asInstanceOf[RankingsClass]
          logInfo("**" + " object: " + " page rank:" + y.pagerank
            + " url: " + y.pageurl
            + " avg duration: " + y.avgduration)

          //based on how I constructed the test data
          assert (y.pagerank==retrieved_kvalues.get(i).intValue())
          assert (y.pageurl== "hello"+ retrieved_kvalues.get(i).intValue())
          assert (y.avgduration ==  retrieved_kvalues.get(i).intValue() + 1)
        }
      }
    }

    assert(actualRetrievedKNumbers == 5)

    val actuals  = new ArrayBuffer[Int]()
    for (i <- 0 to actualRetrievedKNumbers-1) {
      actuals += retrieved_kvalues.get(i)
    }

    val expecteds = Seq (1, 3, 5, 7, 9)
    assert  (expecteds === actuals.toArray)

    reduceSHMShuffleStore.stop()
    reduceSHMShuffleStore.shutdown()
    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

  ignore ("reduceId 96 with class registration") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize()
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0
    val mapId = 1
    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int


    val threadLocalResources = getThreadLocalShuffleResource(conf)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType)

    val reduceId = 96
    val reduceSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer,
        shuffleId, reduceId, numberOfPartitions)

    var mapStatusResult = null.asInstanceOf[ShuffleDataModel.MapStatus]

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()

    for (i <- 0 to numberOfVs-1 ) {
      val obj = new RankingsClass(i, "hello" +i, i+1)
      testObjects.add(obj)
      voffsets.add(0)
      partitions.add( i%2 )
      kvalues.add (i)
    }

    //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
    mapSHMShuffleStore.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
      voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
    //before storeKVpairs, the Value Type needs to be stored already.
    mapSHMShuffleStore.storeVValueType(testObjects.get(0))
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs = numberOfVs
    mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
      kvalues.asInstanceOf[ArrayList[Integer]],
      partitions.asInstanceOf[ArrayList[Integer]],
      numberOfPairs)

    mapStatusResult = mapSHMShuffleStore.sortAndStore()

    logInfo("map status region name: " + mapStatusResult.getShmRegionName())
    logInfo ("map status offset to index chunk: 0x "
      + java.lang.Long.toHexString(mapStatusResult.getOffsetToIndexBucket()))
    val buckets = mapStatusResult.getMapStatus()

    if (buckets != null) {
      for (i <- 0 to buckets.length-1) {
        logInfo ("map status, bucket: " + i + " has size: " + buckets(i))
      }
    }
    else {
      logInfo("map status buckets length is null.")
    }

    //reduce side:
    reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions)
    val mapIds = Seq (mapId).toArray
    val shmRegionNames = Seq (mapStatusResult.getShmRegionName()).toArray
    val offsetToIndexChunks = Seq (mapStatusResult.getOffsetToIndexBucket()).toArray
    val sizes = Seq (mapStatusResult.getMapStatus()(reduceId)).toArray //pick the first bucket

    val statuses =
      new ShuffleDataModel.ReduceStatus(mapIds, shmRegionNames, offsetToIndexChunks, sizes)
    //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet.
    reduceSHMShuffleStore.mergeSort(statuses)

    //retrieve the data
    val retrieved_knumbers = 6
    val retrieved_kvalues = new ArrayList[Integer]()
    val retrieved_vvalues = new ArrayList[ArrayList[Object]] ()
    for (i <- 0 to retrieved_knumbers-1) {
      retrieved_kvalues.add(0); //initialization to 0;
      retrieved_vvalues.add(null); //initialization to null;
    }

    val actualRetrievedKNumbers=
      reduceSHMShuffleStore.getKVPairsWithIntKeys (retrieved_kvalues,
        retrieved_vvalues,
        retrieved_knumbers)

    logInfo("actual number of the keys retrieved is: " + actualRetrievedKNumbers)

    //the retrieved result number is expected to be 0
    for (i <-0 to actualRetrievedKNumbers-1) {
      logInfo("retrieved k value: " + retrieved_kvalues.get(i))
      val tvvalues = retrieved_vvalues.get(i)
      for (m <-0 to tvvalues.size()-1) {
        val x = tvvalues.get(m);
        assert (x.isInstanceOf[RankingsClass])
        if ( x.isInstanceOf[RankingsClass]) {

          val y =  x.asInstanceOf[RankingsClass]
          logInfo("**" + " object: " + " page rank:" + y.pagerank
            + " url: " + y.pageurl
            + " avg duration: " + y.avgduration)

          //based on how I constructed the test data
          assert (y.pagerank==retrieved_kvalues.get(i).intValue())
          assert (y.pageurl== "hello"+ retrieved_kvalues.get(i).intValue())
          assert (y.avgduration ==  retrieved_kvalues.get(i).intValue() + 1)
        }
      }
    }

    assert(actualRetrievedKNumbers == 0)

    reduceSHMShuffleStore.stop()
    reduceSHMShuffleStore.shutdown()
    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }


  test ("merge two map stores with class registration") {
    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    //serialization buffer, to be set as the string.
    conf.set("spark.shuffle.shm.serializer.buffer.max.mb", "64");

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize()
    val  nativePointer = ShuffleStoreManager.INSTANCE.getPointer()
    logInfo ("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer))

    val shuffleId = 0

    val numberOfPartitions = 100
    val keyType = ShuffleDataModel.KValueTypeId.Int

    val threadLocalResources = getThreadLocalShuffleResource(conf)

    val mapId1 = 1
    val mapSHMShuffleStore1 =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId1, numberOfPartitions, keyType)

    val mapId2 = 4
    val mapSHMShuffleStore2 =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId2, numberOfPartitions, keyType)


    val reduceId = 0
    val reduceSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createReduceShuffleStore(
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer,
        shuffleId, reduceId, numberOfPartitions)

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()

    for (i <- 0 to numberOfVs-1 ) {
      val obj = new RankingsClass(i, "hello" +i, i+1)
      testObjects.add(obj)
      voffsets.add(0)
      partitions.add( i%2 )
      kvalues.add (i)
    }

    //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
    mapSHMShuffleStore1.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
      voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
    //before storeKVpairs, the Value Type needs to be stored already.
    mapSHMShuffleStore1.storeVValueType(testObjects.get(0))
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs1 = numberOfVs
    mapSHMShuffleStore1.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
      kvalues.asInstanceOf[ArrayList[Integer]],
      partitions.asInstanceOf[ArrayList[Integer]],
      numberOfPairs1)

    val  mapStatusResult1 = mapSHMShuffleStore1.sortAndStore()

    logInfo("map status region name: " + mapStatusResult1.getShmRegionName())
    logInfo ("map status offset to index chunk: 0x "
      + java.lang.Long.toHexString(mapStatusResult1.getOffsetToIndexBucket()))
    val buckets = mapStatusResult1.getMapStatus()

    if (buckets != null) {
      for (i <- 0 to buckets.length-1) {
        logInfo ("for map store 1, map status, bucket: " + i + " has size: " + buckets(i))
      }
    }
    else {
      logInfo("for map store 1, map status buckets length is null.")
    }

    //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
    mapSHMShuffleStore2.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
      voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
    //before storeKVpairs, the Value Type needs to be stored already.
    mapSHMShuffleStore2.storeVValueType(testObjects.get(0))
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs2 = numberOfVs
    mapSHMShuffleStore2.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
      kvalues.asInstanceOf[ArrayList[Integer]],
      partitions.asInstanceOf[ArrayList[Integer]],
      numberOfPairs2)

    val  mapStatusResult2 = mapSHMShuffleStore2.sortAndStore()

    logInfo("map status region name: " + mapStatusResult2.getShmRegionName())
    logInfo ("map status offset to index chunk: 0x "
      + java.lang.Long.toHexString(mapStatusResult2.getOffsetToIndexBucket()))
    val buckets2 = mapStatusResult2.getMapStatus()

    if (buckets2 != null) {
      for (i <- 0 to buckets2.length-1) {
        logInfo ("for map store 2, map status, bucket: " + i + " has size: " + buckets2(i))
      }
    }
    else {
      logInfo("for map store 2, map status buckets length is null.")
    }



    //reduce side:
    reduceSHMShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions)
    val mapIds = Seq (mapId1, mapId2).toArray
    val shmRegionNames = Seq (mapStatusResult1.getShmRegionName(),
                                   mapStatusResult2.getShmRegionName()).toArray
    val offsetToIndexChunks = Seq (mapStatusResult1.getOffsetToIndexBucket(),
                                         mapStatusResult2.getOffsetToIndexBucket()).toArray
    val sizes = Seq (mapStatusResult1.getMapStatus()(reduceId),
                             mapStatusResult1.getMapStatus()(reduceId)).toArray //pick the first bucket

    val statuses =
      new ShuffleDataModel.ReduceStatus(mapIds, shmRegionNames, offsetToIndexChunks, sizes)
    //NOTE: mergeSort basically is just the preparation. no merge sort actually conducted yet.
    reduceSHMShuffleStore.mergeSort(statuses)

    //retrieve the data
    val retrieved_knumbers = 6
    val retrieved_kvalues = new ArrayList[Integer]()
    val retrieved_vvalues = new ArrayList[ArrayList[Object]] ()
    for (i <- 0 to retrieved_knumbers-1) {
      retrieved_kvalues.add(0); //initialization to 0;
      retrieved_vvalues.add(null); //initialization to null;
    }

    val actualRetrievedKNumbers=
      reduceSHMShuffleStore.getKVPairsWithIntKeys (retrieved_kvalues,
        retrieved_vvalues,
        retrieved_knumbers)

    logInfo("actual number of the keys retrieved is: " + actualRetrievedKNumbers)

    for (i <-0 to actualRetrievedKNumbers-1) {
      logInfo("retrieved k value: " + retrieved_kvalues.get(i))
      val tvvalues = retrieved_vvalues.get(i)
      for (m <-0 to tvvalues.size()-1) {
        val x = tvvalues.get(m);
        assert (x.isInstanceOf[RankingsClass])
        if ( x.isInstanceOf[RankingsClass]) {

          val y =  x.asInstanceOf[RankingsClass]
          logInfo("**" + " object: " + " page rank:" + y.pagerank
            + " url: " + y.pageurl
            + " avg duration: " + y.avgduration)

          //based on how I constructed the test data
          assert (y.pagerank==retrieved_kvalues.get(i).intValue())
          assert (y.pageurl== "hello"+ retrieved_kvalues.get(i).intValue())
          assert (y.avgduration ==  retrieved_kvalues.get(i).intValue() + 1)
        }
      }
    }

    assert(actualRetrievedKNumbers == 5)

    val actuals  = new ArrayBuffer[Int]()
    for (i <- 0 to actualRetrievedKNumbers-1) {
      actuals += retrieved_kvalues.get(i)
    }

    val expecteds = Seq (0, 2, 4, 6, 8)
    assert  (expecteds === actuals.toArray)

    //finally, add the value class definition retrieval

    val typeDefinition = reduceSHMShuffleStore.getVValueType()
    val retrievedClass =  reduceSHMShuffleStore.getVValueTypeClass(typeDefinition)
    logInfo ("retrieved application class is: " + retrievedClass.getName())
    assert (retrievedClass.equals(classOf[RankingsClass]))


    reduceSHMShuffleStore.stop()
    reduceSHMShuffleStore.shutdown()
    mapSHMShuffleStore1.stop()
    mapSHMShuffleStore1.shutdown()
    mapSHMShuffleStore2.stop()
    mapSHMShuffleStore2.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

}
