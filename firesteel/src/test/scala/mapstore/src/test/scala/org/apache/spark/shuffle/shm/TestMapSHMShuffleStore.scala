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
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.FunSuite
import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf, Logging}
import org.apache.spark.serializer._
import com.hp.hpl.firesteel.shuffle._
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer._
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList

/**
 * Created by junli on 7/24/2015.
 */

class TestMapSHMShuffleStore extends FunSuite with LocalSparkContext with Logging {

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

  test ("loading shuffle store manager only") {
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
    val keyType = ShuffleDataModel.KValueTypeId.String


    val threadLocalResources = getThreadLocalShuffleResource(conf)
    val serializer =
      new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType);

    mapSHMShuffleStore.stop();
    mapSHMShuffleStore.shutdown();
    ShuffleStoreManager.INSTANCE.shutdown();

    sc.stop()
  }

  //use variable sc instead.
  test ("loading shuffle store manager with serialization of map data and class registration") {

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
    val serializer =
      new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType);

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()
    for (i <- 0 to numberOfVs-1) {
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
    mapSHMShuffleStore.storeVValueType(testObjects.get(0));
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs = numberOfVs;
    mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
          kvalues.asInstanceOf[ArrayList[Integer]],
          partitions.asInstanceOf[ArrayList[Integer]],
          numberOfPairs);

    val mapStatusResult = mapSHMShuffleStore.sortAndStore();

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
    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()

    sc.stop()
  }

  //use variable sc instead.
  test ("loading shuffle store manager with serialization of map data without class registration") {

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
    val serializer =
      new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (
        threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer)

    val mapSHMShuffleStore =
      ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
        threadLocalResources.getByteBuffer,
        shuffleId, mapId, numberOfPartitions, keyType);

    val numberOfVs = 10
    val testObjects = new ArrayList[RankingsClass] ()
    val partitions = new ArrayList[Int]()
    val kvalues = new ArrayList[Int] ()
    val voffsets = new ArrayList[Int] ()
    for (i <- 0 to numberOfVs-1) {
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
    mapSHMShuffleStore.storeVValueType(testObjects.get(0));
    // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
    //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
    val numberOfPairs = numberOfVs;
    mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
      kvalues.asInstanceOf[ArrayList[Integer]],
      partitions.asInstanceOf[ArrayList[Integer]],
      numberOfPairs);

    val mapStatusResult = mapSHMShuffleStore.sortAndStore();

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

    mapSHMShuffleStore.stop()
    mapSHMShuffleStore.shutdown()
    ShuffleStoreManager.INSTANCE.shutdown()
    sc.stop()
  }


  //use variable sc instead.
  test ("loading shuffle store manager with serialization of map data with repeated invocation") {

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

    val repeatedNumber = 2
    for (k <- 0 to repeatedNumber -1) {
      logInfo ("**************repeat number*********************" + k)

      val threadLocalResources = getThreadLocalShuffleResource(conf)
      val serializer =
        new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer(
          threadLocalResources.getKryoInstance, threadLocalResources.getByteBuffer)

      val mapSHMShuffleStore =
        ShuffleStoreManager.INSTANCE.createMapShuffleStore(threadLocalResources.getKryoInstance,
          threadLocalResources.getByteBuffer,
          shuffleId, mapId, numberOfPartitions, keyType);

      val numberOfVs = 10
      val testObjects = new ArrayList[RankingsClass]()
      val partitions = new ArrayList[Int]()
      val kvalues = new ArrayList[Int]()
      val voffsets = new ArrayList[Int]()
      for (i <- 0 to numberOfVs - 1) {
        val obj = new RankingsClass(i, "hello" + i, i + 1)
        testObjects.add(obj)
        voffsets.add(0)
        partitions.add(i % 2)
        kvalues.add(i)
      }

      //serializeVs (ArrayList<Object> vvalues, ArrayList<Integer> voffsets, int numberOfVs)
      mapSHMShuffleStore.serializeVs(testObjects.asInstanceOf[ArrayList[Object]],
        voffsets.asInstanceOf[ArrayList[Integer]], numberOfVs)
      //before storeKVpairs, the Value Type needs to be stored already.
      mapSHMShuffleStore.storeVValueType(testObjects.get(0));
      // storeKVPairsWithIntKeys (ArrayList<Integer> voffsets,
      //ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs)
      val numberOfPairs = numberOfVs;
      mapSHMShuffleStore.storeKVPairsWithIntKeys(voffsets.asInstanceOf[ArrayList[Integer]],
        kvalues.asInstanceOf[ArrayList[Integer]],
        partitions.asInstanceOf[ArrayList[Integer]],
        numberOfPairs);

      val mapStatusResult = mapSHMShuffleStore.sortAndStore();

      logInfo("map status region name: " + mapStatusResult.getShmRegionName())
      logInfo("map status offset to index chunk: 0x "
        + java.lang.Long.toHexString(mapStatusResult.getOffsetToIndexBucket()))
      val buckets = mapStatusResult.getMapStatus()

      if (buckets != null) {
        for (i <- 0 to buckets.length - 1) {
          logInfo("map status, bucket: " + i + " has size: " + buckets(i))
        }
      }
      else {
        logInfo("map status buckets length is null.")
      }

      mapSHMShuffleStore.stop()
      mapSHMShuffleStore.shutdown()
    }


    ShuffleStoreManager.INSTANCE.shutdown()
    sc.stop()
  }
}


//NOTE: this is a more concise way to define a simple data class
case class RankingsClass (pagerank: Int,
                          pageurl: String,
                          avgduration: Int)

//NOTE: both case class and register class will have to be at the outer-most class scope. If I
//move these two into the test class private scope. It does not work!!!
class MyRegistrator extends KryoRegistrator with Logging  {

  def registerClasses (k: Kryo) {
    var registered = false
    try {
      k.register(classOf[RankingsClass])
      registered = true
    }
    catch {
      case e: Exception =>
        logError ("fails to register via MyRegistrator", e)
    }

    if (registered) {
      logInfo("in test suite, successfully register class RankingsClass")
    }
  }
}