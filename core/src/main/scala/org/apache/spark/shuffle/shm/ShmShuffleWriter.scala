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

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.Logging
import org.apache.spark.TaskContext

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.SparkEnv;
import org.apache.spark.storage.BlockManagerId

import com.hp.hpl.firesteel.shuffle._

import java.nio.ByteBuffer
import java.util.List
import java.util.ArrayList
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.lang.{String => JString}


import scala.collection.mutable.ArrayBuffer

/**
 * Shuffle writer designed for shared-memory based access.
 */
private[spark] class ShmShuffleWriter[K, V]( shuffleStoreMgr:ShuffleStoreManager,
                                             handle: BaseShuffleHandle[K, V, _],
                                             mapId: Int,
                                             context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  //to mimic what is in SortShuffleWriter
  private var stopping = false
  private var mapStatus: MapStatus= null

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions

  private val shuffleId=handle.shuffleId
  private val numberOfPartitions=handle.dependency.partitioner.numPartitions

  //we will introduce a Spark configuraton parameter for this one. and serialziation and
  //deserialization buffers should be the same size, as we need to support thread re-use.
  private val SERIALIZATION_BUFFER_SIZE = 1*1024*1024*1024
  //we batch 5000 records before we go to the write.This can be configurable later.
  private val SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE = 2000
  //we will pool the kryo instance and ByteBuffer instance later.

  //per shuffle task resource
  private val threadLocalShuffleResource = getThreadLocalShuffleResource()
  private var mapShuffleStore = null.asInstanceOf[MapSHMShuffleStore]

  private val kholder = new ArrayList[Any](SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE)
  private val vholder = new ArrayList[Any](SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE)
  //to hold partition number for each map bucket.
  private val partitions = new ArrayList[Integer] (SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE)

  //to record the map output status that will be passed to the scheduler and map output tracker.
  private val blockManager = SparkEnv.get.blockManager

  //to check whether ordering and aggregation is defined.
  private val ordering = dep.keyOrdering.isDefined
  //no need to determine aggregation at the map side
  //private val aggregation = dep.aggregator.isDefined

  private def getThreadLocalShuffleResource():
                            ThreadLocalShuffleResourceHolder.ShuffleResource = {
       val resourceHolder= new ThreadLocalShuffleResourceHolder()
       var shuffleResource = resourceHolder.getResource()
       if (shuffleResource == null) {
          val kryoInstance =  new KryoSerializer(SparkEnv.get.conf).newKryo(); //per-thread
          val serializationBuffer = ByteBuffer.allocateDirect(SERIALIZATION_BUFFER_SIZE)
          if (serializationBuffer.capacity() != SERIALIZATION_BUFFER_SIZE ) {
            logError("Thread: " + Thread.currentThread().getId
              + " created serialization buffer with size: "
              + serializationBuffer.capacity()
              + ": FAILED to match: " + SERIALIZATION_BUFFER_SIZE)
          }
          else {
            logInfo("Thread: " + + Thread.currentThread().getId
              + " created the serialization buffer with size: "
              + SERIALIZATION_BUFFER_SIZE + ": SUCCESS")
          }
          //add a logical thread id
          val logicalThreadId = ShuffleStoreManager.INSTANCE.getlogicalThreadCounter ()
          resourceHolder.initilaze(kryoInstance, serializationBuffer, logicalThreadId)
          shuffleResource = resourceHolder.getResource()

          logDebug ("Thread: " + Thread.currentThread().getId
                                      + " create kryo-bytebuffer resource for mapper writer")
       }
       shuffleResource
  }

  private def createMapShuffleStore[K](firstK: K): MapSHMShuffleStore ={
    mapShuffleStore = {
      firstK match {
        case intValue: Int => {
          //initialize the kholder and vholder
          (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
            kholder.add(0)
            vholder.add(null.asInstanceOf[V])
          }
          shuffleStoreMgr.createMapShuffleStore(
            threadLocalShuffleResource.getKryoInstance(),
            threadLocalShuffleResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Int, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case longValue: Long => {
          (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
            kholder.add(0L)
            vholder.add(null.asInstanceOf[V])
          }
          shuffleStoreMgr.createMapShuffleStore(
            threadLocalShuffleResource.getKryoInstance(),
            threadLocalShuffleResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Long, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case floatValue: Float => {
          (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
            kholder.add(0.0)
            vholder.add(null.asInstanceOf[V])
          }

          shuffleStoreMgr.createMapShuffleStore(
            threadLocalShuffleResource.getKryoInstance(),
            threadLocalShuffleResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Float, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case doubleValue: Double => {
          (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
            kholder.add(0.0)
            vholder.add(null.asInstanceOf[V])
          }
          throw  new NotImplementedError("we have not implemented this method yet")
        }
        case stringValue: String => {
          (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
            kholder.add("")
            vholder.add(null.asInstanceOf[V])
          }
          shuffleStoreMgr.createMapShuffleStore(
            threadLocalShuffleResource.getKryoInstance(),
            threadLocalShuffleResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.String, SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
        case _ =>  {
          (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
            kholder.add(null.asInstanceOf[K])
            vholder.add(null.asInstanceOf[V])
          }
          shuffleStoreMgr.createMapShuffleStore(
            threadLocalShuffleResource.getKryoInstance(),
            threadLocalShuffleResource.getByteBuffer(),
            threadLocalShuffleResource.getLogicalThreadId,
            shuffleId, mapId,numberOfPartitions,
            ShuffleDataModel.KValueTypeId.Object,SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE,
            ordering) //true to allow sort/merge-sort with ordering.
        }
      }
    }

    mapShuffleStore
  }


  override def write (records: Iterator[_<: Product2[K,V]]): Unit = {
    var kv: Product2[K, V] = null
    val bit = records.buffered

    //to handle the case where the partition has zero size.
    if (bit.hasNext) {
      val firstKV = bit.head //not advance to the next.

      (0 until SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE).map { mapId =>
        partitions.add(0)
      }

      mapShuffleStore = createMapShuffleStore(firstKV._1)

      var count: Int = 0
      //NOTE: we can not use records for iteration--It will miss the first value.
      while (bit.hasNext) {
        kv = bit.next()
        kholder.set(count, kv._1)
        vholder.set(count, kv._2)
        partitions.set(count, handle.dependency.partitioner.getPartition(kv._1))

        count = count + 1
        if (count == SHUFFLE_STORE_BATCH_SERIALIZATION_SIZE) {
          pushKVpairsToBuffers(kv._1, kv._2, count)
          count = 0 //reset
        }
      }

      if (count > 0) {
        //some lefover
        pushKVpairsToBuffers(kv._1, kv._2, count)
      }
    }
    else {
      val firstKV = (0, 0) //make it an integer.
      //NOTE: such that map shuffle store is created with an integer key type. but the C++ engine
      //that pick up the type should not use the channel (bucket) that has zero size bucket to
      //determine the type.
      mapShuffleStore = createMapShuffleStore(firstKV._1)
    }

    //when done, issue sort and store and get the map status information
    val mapStatusResult = mapShuffleStore.sortAndStore()

    logInfo( "store id" + mapShuffleStore.getStoreId
         + " shm-shuffle map status region id: " + mapStatusResult.getRegionIdOfIndexBucket())
    logInfo("store id" + mapShuffleStore.getStoreId
         + " shm-shuffle map status offset to index chunk: 0x "
                      + java.lang.Long.toHexString(mapStatusResult.getOffsetOfIndexBucket()))
    val blockManagerId = blockManager.shuffleServerId
    //unlike the original spark shuffle, here, block manager is mutable for each map task.
    //WARNING: still, we need to modify map status logic to accomodate remote exexutor launching
    //due to block manager's cache look up.
    val clonedBlockManagerId =
       BlockManagerId(blockManagerId.executorId, blockManagerId.host, blockManagerId.port,
         mapStatusResult.getRegionIdOfIndexBucket(),
         mapStatusResult.getOffsetOfIndexBucket()
       )

    val partitionLengths= mapStatusResult.getMapStatus ()
    mapStatus = MapStatus(clonedBlockManagerId, partitionLengths)
  }

  private def pushKVpairsToBuffers[K,V](kvalue:K, vvalue:V, count: Int): Unit={
     kvalue match {
       case intValue: Int => {
         mapShuffleStore.serializeVs(vholder.asInstanceOf[ArrayList[Object]], count)
         mapShuffleStore.storeVValueType(vvalue)//need to store the value's type
         mapShuffleStore.storeKVPairsWithIntKeys(
                     kholder.asInstanceOf[ArrayList[Integer]], partitions, count)
       }

       case longValue: Long => {
         mapShuffleStore.serializeVs(vholder.asInstanceOf[ArrayList[Object]], count)
         mapShuffleStore.storeVValueType(vvalue)//need to store the value's type
         mapShuffleStore.storeKVPairsWithLongKeys(
                     kholder.asInstanceOf[ArrayList[JLong]], partitions, count)
       }
       case floatValue: Float => {
         mapShuffleStore.serializeVs(vholder.asInstanceOf[ArrayList[Object]], count)
         mapShuffleStore.storeVValueType(vvalue)//need to store the value's type
         mapShuffleStore.storeKVPairsWithFloatKeys(
                     kholder.asInstanceOf[ArrayList[JFloat]], partitions, count)
       }
       case doubleValue: Double => {
         throw  new NotImplementedError("we have not implemented this method yet")
       }
       case stringValue: String => {
         mapShuffleStore.serializeVs(vholder.asInstanceOf[ArrayList[Object]], count)
         mapShuffleStore.storeVValueType(vvalue)//need to store the value's type
         mapShuffleStore.storeKVPairsWithStringKeys(
                      kholder.asInstanceOf[ArrayList[JString]], partitions, count)
       }
       case _ =>  {
          //it is an object based array
          mapShuffleStore.serializeKVPairs(kholder.asInstanceOf[ArrayList[Object]],
                       vholder.asInstanceOf[ArrayList[Object]],count)
          //TODO: this case require stores both the key's type and values' type. need testing.
          mapShuffleStore.storeKVTypes(kvalue, vvalue)//need to store the value's type
          mapShuffleStore.storeKVPairs(partitions, count);
       }
     }
  }

  /**
   * stop will force the completion of the shuffle write and return
   * MapStatus information.
   *
   * We will produce MapStatus information exactly like what is today, as otherwise, we will have
   * to change MapOutputTracker logic and corresponding data structures to do so.
   *
   * @param success when we get here, we already know that we can stop it with success or not.
   * @return record map shuffle status information that will be sent to the job scheduler
   */
  override def stop(success: Boolean): Option[MapStatus]= {
      try {
        if (stopping) {
          return None
        }
        stopping = true
        if (success) {
          return Option (mapStatus)
        }
        else {
          //TODO: we will need to remove the NVM data produced by this failed Map task.
          return None;
        }
      }finally {
        //In sort-based shuffle, current sort shuffle writer stop the sorter, which is to clean up
        //the intermediate files
        //For shm-based shuffle, to shutdown the shuffle store for this map task (basically to
        // clean up the occupied DRAM based resource. but not NVM-based resource, which will be
        // cleaned up during unregister the shuffle.
        mapShuffleStore.stop();

      }

  }
}
