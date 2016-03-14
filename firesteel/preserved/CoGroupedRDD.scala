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
 */

package org.apache.spark.rdd

import java.util.Comparator

import scala.collection.mutable
import scala.language.existentials

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{InterruptibleIterator, Partition, Partitioner, SparkEnv, TaskContext}
import org.apache.spark.{Dependency, OneToOneDependency, ShuffleDependency}
//Added by Jun Li
import org.apache.spark.Aggregator
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{ExternalAppendOnlyMap, AppendOnlyMap, CompactBuffer}
import org.apache.spark.util.Utils
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

private[spark] sealed trait CoGroupSplitDep extends Serializable

private[spark] case class NarrowCoGroupSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition
  ) extends CoGroupSplitDep {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] case class ShuffleCoGroupSplitDep(handle: ShuffleHandle) extends CoGroupSplitDep

private[spark] class CoGroupPartition(idx: Int, val deps: Array[CoGroupSplitDep])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

//modified by Jun Li, to support the co-group of the multiple RDDs' iterators, each of which is
//already sorted already.
private [spark] class PriQueueBasedSorter[K, _]
                    (val rddIterators:ArrayBuffer[Iterator[Product2[K, Iterable[_]]]],
                        val numberOfGroupedRdds: Int)
                 extends Iterable[(K, _)]{

  val bufferedIters =
      rddIterators.zipWithIndex.filter(_._1.hasNext).map(x => (x._1.buffered, x._2))
  //we need the index to know which rdd will be put to which array.
  type Iter = (BufferedIterator[Product2[K, Iterable[_]]], Int)
  //rely on the implicit ordering of K to do the work.
  //to decide whether we will use ordering, or reverse ordering.
  val heap = new mutable.PriorityQueue[Iter]()(
    new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      // The correct way is to create the OrderedCoGroupRDD, so that we can specify that K
      // supports Ordering in the constructor.
      override def compare(x: Iter, y: Iter) : Int ={
        (x._1.head._1, y._1.head._1) match {
          case (av: Int, bv: Int) =>
            if (av > bv) -1 else 1

          case (av: Long, bv: Long) =>
            if (av > bv) -1 else 1

          case (av: Float, bv: Long) =>
            if (av > bv) -1 else 1

          case (av: Double, bv: Double) =>
            if (av > bv) -1 else 1

          case (av: Short, bv: Short) =>
            if (av > bv) -1 else 1

          case (av: String, bv: String) =>
            -av.compare(bv)
          case _ =>
            throw  new Exception(s"key will need to have ordering function to be support")
        }

      }
    })

  heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true

  val mergeSortIterator =  new Iterator[(Product2[K, Iterable[_]], Int)] {
    override def hasNext: Boolean = !heap.isEmpty

    override def next(): (Product2[K, Iterable[_]], Int) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val (firstBuf, qindex) = heap.dequeue()
      val firstPair= firstBuf.next()
      if (firstBuf.hasNext) {
        heap.enqueue((firstBuf, qindex))
      }
      (firstPair, qindex)
    }
  }

  //should that be: Product2[K, Seq[V]]
  //the return should be priQueue.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]
  override def iterator: Iterator[(K, Array[Iterable[_]])] = {
    val sorted = mergeSortIterator.buffered //in order to inspect the head.

    //val array = new Array[Partition](part.numPartitions)
    new Iterator[(K, Array[Iterable[_]])] {
      def nextValue(): (Product2[K, Iterable[_]], Int) = {
         sorted.next() //this has to be invoked from sorted.
      }

      override def hasNext: Boolean = sorted.hasNext

      //not all of the array elements will be filled, depending on how key is distributed to
      //different RDDs
      override def next(): (K, Array[Iterable[_]]) =
      {
        //NOTE: should we fill the array with not null empty list?
        val array = new Array[Iterable[_]](numberOfGroupedRdds)
        //so that I do not have null value to start with.
        //val array= Array.fill(numberOfGroupedRdds)(Array.empty[_])
        val (value, qindex) = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator for rdd with index: " + qindex)
        }

        array(qindex)=value._2
        val k = value._1
        while (sorted.hasNext && sorted.head._1._1 == k) {
           val seqValues = sorted.head._1._2
           val rddIndex = sorted.head._2
           array(rddIndex) = seqValues
           sorted.next() //advance to next sorted value.
        }

        for (i <- 0 until numberOfGroupedRdds ) {
          if (array(i) == null) {
            array(i) = Array.empty[Any]
          }
        }
        (k, array)
      }
    }
  }
}


/**
 * :: DeveloperApi ::
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * Note: This is an internal API. We recommend users use RDD.coGroup(...) instead of
 * instantiating this directly.

 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 */
@DeveloperApi
class CoGroupedRDD[K](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {

  // For example, `(k, a) cogroup (k, b)` produces k -> Seq(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Seq as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  private type CoGroup = CompactBuffer[Any]
  private type CoGroupValue = (Any, Int)  // Int is dependency number
  private type CoGroupCombiner = Array[CoGroup]

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_ <: Product2[K, _]] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        val createCombiner = (v: Any) => CompactBuffer(v)
        val mergeValue = (buf: CompactBuffer[Any], v: Any) => buf += v
        val mergeCombiners = (c1: CompactBuffer[Any], c2: CompactBuffer[Any]) => c1 ++= c2
        val aggregator =
          new Aggregator[K, Any, CompactBuffer[Any]](createCombiner, mergeValue, mergeCombiners)
        //updated by Jun Li, for chortcircuit the combiner for aggregation
        val shortcircuitCombiner=true
        aggregator.shortcircuitCombiner(shortcircuitCombiner)

        //also I need ordering to trigger sort/merge-sort scheme in C++ shuffle engine
        val orderF = new Ordering[K] {
          // Use the reverse of comparator.compare because PriorityQueue dequeues the max
          override def compare(a: K, b: K): Int = {
            (a, b) match {
              case (av: Int, bv: Int) =>
                if (av > bv) 1 else -1

              case (av: Long, bv: Long) =>
                if (av > bv) 1 else -1

              case (av: Float, bv: Long) =>
                if (av > bv) 1 else -1

              case (av: Double, bv: Double) =>
                if (av > bv) 1 else -1

              case (av: String, bv: String) =>
                av.compare(bv)
              case _ =>
                throw new Exception(s"key will need to have ordering function to be support")
            }
          }
        }
        //I need this one to control the merging at the reducer side.
        new ShuffleDependency[K, Any, CompactBuffer[Any]](rdd, part, serializer,
          Some(orderF), aggregator=Some(aggregator), mapSideCombine=false)

        //new ShuffleDependency[K, Any, CoGroupCombiner](rdd, part, serializer)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleHandle)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true)
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) =>
        // Read them from the parent
        //Jun Li: with sort and merge-based shuffle, the result will satisfy
        //asInstanceOf[Iterator[Product2[K, Iterable[V]]]
        val it = rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]]
        rddIterators += ((it, depNum))

      case ShuffleCoGroupSplitDep(handle) =>
        // Read map outputs of shuffle
        val it = SparkEnv.get.shuffleManager
          .getReader(handle, split.index, split.index + 1, context)
          .read()
        rddIterators += ((it, depNum))
    }

    //modified by Jun Li, to meet the result that what we created from the sort-based shuffle
    // engine is actually sorted with order already.
    var iteratorOrdered = true
    val rddIteratorsWithSeqV = new ArrayBuffer[Iterator[Product2[K, Iterable[_]]]]
    //check whether each iterators are OK to be seq[V].
    for ((it, depNum) <- rddIterators) {
      it match {
        //Iterator[Product2[K, Iterable[_]]]
        case m: Iterator[Product2[K, Iterator[_]]] =>
          rddIteratorsWithSeqV += it.asInstanceOf[Iterator[Product2[K, Iterable[_]]]]
        case _ =>
          iteratorOrdered = false
      }
    }

    if (iteratorOrdered) {
      logInfo("CoGroupedRDD chooses priority-queue/merge-sort for co-group operation")

      val priQueue = new PriQueueBasedSorter(rddIteratorsWithSeqV, numRdds)
      new InterruptibleIterator (context,
                  priQueue.iterator.asInstanceOf[Iterator[(K, Array[Iterable[Any]])]])
    }
    else {
      logInfo("CoGroupedRDD still chooses hash-based approach for co-group operation")

      if (!externalSorting) {
        val map = new AppendOnlyMap[K, CoGroupCombiner]
        val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
          if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup)
        }
        val getCombiner: K => CoGroupCombiner = key => {
          map.changeValue(key, update)
        }
        rddIterators.foreach { case (it, depNum) =>
          while (it.hasNext) {
            val kv = it.next()
            getCombiner(kv._1)(depNum) += kv._2
          }
        }
        new InterruptibleIterator(context,
          map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
      } else {
        val map = createExternalMap(numRdds)
        for ((it, depNum) <- rddIterators) {
          map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
        }
        context.taskMetrics.memoryBytesSpilled += map.memoryBytesSpilled
        context.taskMetrics.diskBytesSpilled += map.diskBytesSpilled
        new InterruptibleIterator(context,
          map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
      }
    }
  }

  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {

    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1
      newCombiner
    }
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }
    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {
        var depNum = 0
        while (depNum < numRdds) {
          combiner1(depNum) ++= combiner2(depNum)
          depNum += 1
        }
        combiner1
      }
    new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
