/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Portions of this file are (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 */

package org.apache.spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * This class represent an unique identifier for a BlockManager.
 *
 * The first 2 constructors of this class is made private to ensure that BlockManagerId objects
 * can be created only using the apply method in the companion object. This allows de-duplication
 * of ID objects. Also, constructor parameters are private to ensure that parameters cannot be
 * modified from outside this class.
 */
@DeveloperApi
class BlockManagerId private (
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int)
  extends Externalizable with Logging {

  private def this() = this(null, null, 0)  // For deserialization only

  def executorId: String = executorId_

  if (null != host_){
    Utils.checkHost(host_, "Expected hostname")
    assert (port_ > 0)
  }

  def hostPort: String = {
    // DEBUG code
    Utils.checkHost(host)
    assert (port > 0)
    host + ":" + port
  }

  def host: String = host_

  def port: Int = port_

  def isDriver: Boolean = { executorId == SparkContext.DRIVER_IDENTIFIER }

  //Added by HPL, to extend the attributes with shared-memmory region Id and also chunkOffset
  //required for shared-memory based shuffling. This is a short-term solution so that we do not
  //need to change the MapOutputTracker's signature to communicate between master and executor.
  var shmRegionId_ = 0L
  var chunkOffset_ = 0L

  def shmRegionId: Long= shmRegionId_
  def chunkOffset: Long = chunkOffset_

  def shmRegionId(id: Long): Unit = {
      shmRegionId_ = id
      if (log.isDebugEnabled) {
        val threadId = Thread.currentThread().getId()
        logDebug("at BlockManagerId shmRegion id assignment: " + shmRegionId_
          + " at thread id: " + threadId)
      }
  }
  def chunkOffset(offset: Long): Unit = {
      chunkOffset_ = offset
      if (log.isDebugEnabled()) {
        val threadId = Thread.currentThread().getId()
        logDebug("at BlockManagerId chunk offset assignment: 0x" + chunkOffset_
          + " at thread id: " + threadId)
      }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    //added by HPL
    out.writeLong(shmRegionId)
    out.writeLong(chunkOffset)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    //added by HPL
    shmRegionId_ = in.readLong()
    chunkOffset_ = in.readLong()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString = s"BlockManagerId($executorId, $host, $port, $shmRegionId, $chunkOffset)"

  override def hashCode: Int = (executorId.hashCode * 41 + host.hashCode) * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId =>
      executorId == id.executorId && port == id.port && host == id.host
    case _ =>
      false
  }
}


private[spark] object BlockManagerId extends Logging {

  /**
   * Returns a [[org.apache.spark.storage.BlockManagerId]] for the given configuration.
   *
   * @param execId ID of the executor.
   * @param host Host name of the block manager.
   * @param port Port of the block manager.
   * @return A new [[org.apache.spark.storage.BlockManagerId]].
   */
  def apply(execId: String, host: String, port: Int) =
    getCachedBlockManagerId(new BlockManagerId(execId, host, port))

  def apply(execId: String, host: String, port: Int, region: Long, offset: Long) ={
    val obj = new BlockManagerId(execId, host, port)
    obj.shmRegionId_ = region
    obj.chunkOffset_ = offset
    obj
  }

  def apply(in: ObjectInput) = {
    val obj = new BlockManagerId()
    obj.readExternal(in)
    //NOTE (HPL): we still need the following method invocation, even though cached id is not
    //the returned result.
    val cachedId = getCachedBlockManagerId(obj)
    //NOTE: modified by HPL.
    //this method is turned on when doing distributed multi-processed executors.
    //thus, region id and chunkoffsets gets initialized if we call getCachedBlockManager(.)
    //on above method. We need to get back the true results, instead of retrieving from cache.
    //logInfo("at apply from io serialization, with id's region id: "
    //                  + obj.shmRegionId + " and offset: " + obj.chunkOffset
    //                  + " and executor id: " + obj.executorId
    //                  + " and host:port:  " + obj.hostPort)
    obj
  }

  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId, BlockManagerId]()

  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    //NOTE: modified by HPL
    //logInfo("before getCachedBlockManagerId called, with id:  " + id.toString)
    blockManagerIdCache.putIfAbsent(id, id)
    val returned_id = blockManagerIdCache.get(id)
    //NOTE: modified by HPL
    //logInfo("after getCachedBlockManagerId called, with id:  " + returned_id.toString)
    returned_id
  }
}
