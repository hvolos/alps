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

import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.{SparkConf, TaskContext, ShuffleDependency}
import org.apache.spark.shuffle._
import org.apache.spark.Logging

import com.hp.hpl.firesteel.shuffle.{ShuffleStoreManager, MapSHMShuffleStore, ShuffleDataModel}

/**
 * Shared-memory based Shuffle Manager
 */
private[spark] class ShmShuffleManager (conf: SparkConf) extends ShuffleManager with Logging{
  //NOTE: this will need to be changed to the maximum cores that an executor can handle
  //in standard Spark specification. It is called "SPARK_WORKER_CORES".
  //NOTE: 2/23/2016, on DragonHawk, it seems that thread pools allocate more threads than specified 
  //(32), so we bump out to 64 entries for now.
  //private val maxNumberOfTaskThreads = conf.getInt("SPARK_WORKER_CORES", 32)
  private val maxNumberOfTaskThreads = conf.getInt("SPARK_WORKER_CORES", 64)

  //global heap name, which requires to be mapped by each executor process.
  private val globalHeapName = conf.get("spark.executor.shm.globalheap.name")

  private var shuffleStoreManager_ = null.asInstanceOf[ShuffleStoreManager]

  def initialize(isLocal: Boolean): Unit = {
    //executor Id, required for shm memory region management.
    //note that for local-process-based Spark, the value is "driver", not the actual executor id.
    logInfo("ShmShuffleManager obtains SPARK_WORKER_CORES: " + maxNumberOfTaskThreads)

    val executorIdStringVal = conf.get("spark.executor.id")
    if (executorIdStringVal.equals("driver")) {
      //only in the local mode and the process is the driver, then
      if (isLocal) {
        logInfo("to initialize shm shuffle manager in local driver,wth executor id string value:"
                        + executorIdStringVal)
        val executorId = 0
        //only one.
        shuffleStoreManager_ = ShuffleStoreManager.INSTANCE.initialize(globalHeapName,
                                                           maxNumberOfTaskThreads, executorId)
      }
    }
    else {
      logInfo("to initialize shm shuffle manager in an executor, with executor id string value:"
                      + executorIdStringVal)
      val executorId = conf.getInt("spark.executor.id", 0)
      shuffleStoreManager_ =
          ShuffleStoreManager.INSTANCE.initialize(globalHeapName,maxNumberOfTaskThreads,executorId)
    }

  }

  def shuffleStoreManager: ShuffleStoreManager  = shuffleStoreManager_

  private val shmShuffleBlockManager =new ShmShuffleBlockManager(conf)

  override def registerShuffle[K, V, C](
                                         shuffleId: Int,
                                         numMaps: Int,
                                         dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getReader[K, C]( 
                                handle: ShuffleHandle,
                                startPartition: Int,
                                endPartition: Int,
                                context: TaskContext): ShuffleReader[K, C] = {
    new ShmShuffleReader(shuffleStoreManager,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)

  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
  : ShuffleWriter[K, V] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    new ShmShuffleWriter(shuffleStoreManager, baseShuffleHandle, mapId, context)

  }

  /**
   * to handle the clean up of the shuffle related permenant data. In our C++ SHM shuffle, it is
   * to clean up the data stored on the NVM at the Map side. In the Map-Reduce model, the Reduce
   * side does not have things needed for preservation until the shuffle stage. Thus, the Reduce
   * side will have all of the resources (DRAM, NVM) to be cleaned up at the "stop" side, including
   * the removal of the reduce-side shuffle store.
   *
   * @param shuffleId
   * @return
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (shuffleStoreManager != null) {
      logInfo("shm-shuffle store manager cleanup map stores with shuffle id: " + shuffleId)
      //NOTE: 2/23/2016, to comment out the cleanup invocation temporarily, as for large-scale
      //BP application testing on DragonHawk, it does not work.
      //shuffleStoreManager.cleanup(shuffleId)
    }
    else {
      logInfo("unregister shuffle unnecessary for shuffle manager on driver in cluster mode!")
    }

    true
  }

  /**
   * mostly to make compiler happy at this time, as fetching blocks is done in C++ shuffle engine,
   * instead of relying on ShmShuffleBlockManager.
   * @return
   */
  override def shuffleBlockManager: ShmShuffleBlockManager = {
    shmShuffleBlockManager
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    //do nothing at this time, we will use it to release the shared-memory resources later.
  }
}
