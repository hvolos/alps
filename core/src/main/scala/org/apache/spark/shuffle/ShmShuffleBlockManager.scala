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

package org.apache.spark.shuffle

import java.nio.ByteBuffer

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{SparkConf, SparkEnv}

/**
 * A dummy implementation at this time, to make the compiler happy, as for shared-memory based
 * C++ engine, we do not want to have Scala side to handle actual block-based data movement.
 * In Spark, special type of block manager is called via BlockManager --> Shuffle Manager-->
 * concrete type of ShuffleBlockManager. For hashed-based shuffling, it will use
 * FileShuffleBlockManager; for sort-based shuffling, it will use IndexShuffleBlockManager.
 */
private [spark] class ShmShuffleBlockManager (conf: SparkConf) extends ShuffleBlockManager  {

  override def getBytes(blockId: ShuffleBlockId): Option[ByteBuffer] = {
     throw  new NotImplementedError("we do not need to implement this block manager currently")
  }

  /**
   * to fetch data from remote block manager.
   * @param blockId
   * @return
   */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    throw  new NotImplementedError("we do not need to implement this block manager currently")
  }

  /**
   *NOTE: this method is added for HP Labs shared-file-system-based shuffle.
   */
  //override
  def getBlockData(blkMgrId: BlockManagerId, blockId: ShuffleBlockId): ManagedBuffer= {
   throw  new NotImplementedError("we do not need to implement this block manager currently")
  }


  override def stop(): Unit = {
    throw new NotImplementedError("we do not need to implemnet this block manager currently")
  }

}
