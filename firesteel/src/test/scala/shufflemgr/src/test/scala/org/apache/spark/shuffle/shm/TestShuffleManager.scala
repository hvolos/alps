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

class TestShuffleManager extends FunSuite with LocalSparkContext with Logging {

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

  //use variable sc instead.
  test("loading shuffle store manager") {

    val conf = new SparkConf(false)
    //to supress a null-pointer from running the test case.
    conf.set("spark.shuffle.manager", "shm")
    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    assert(shuffleManager.isInstanceOf[ShmShuffleManager])

    ShuffleStoreManager.INSTANCE.initialize();
    val nativePointer = ShuffleStoreManager.INSTANCE.getPointer();
    logInfo("native pointer of shuffle store manager retrieved is:"
      + "0x"+ java.lang.Long.toHexString(nativePointer));
    ShuffleStoreManager.INSTANCE.shutdown();

    sc.stop()
  }

}
