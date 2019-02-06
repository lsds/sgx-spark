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
 */
package org.apache.spark.sgx

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.sgx.iterator.{SgxFakeIterator, SgxIteratorIdentifier}
import org.apache.spark.util.collection.{PartitionedAppendOnlyMap, SizeTrackingAppendOnlyMapIdentifier}
import org.apache.spark.util.random.RandomSampler

object SgxIteratorFct {
  
	def computeMapPartitionsRDD[U, T](id: SgxIteratorIdentifier[T], fct: (Int, Iterator[T]) => Iterator[U], partIndex: Int) =
		new ComputeMapPartitionsRDD[U, T](id, fct, partIndex).send()

	def computePartitionwiseSampledRDD[T, U](it: SgxIteratorIdentifier[T], sampler: RandomSampler[T, U]) =
		new ComputePartitionwiseSampledRDD[T, U](it, sampler).send()

	def computeZippedPartitionsRDD2[A, B, Z](a: SgxIteratorIdentifier[A], b: SgxIteratorIdentifier[B], fct: (Iterator[A], Iterator[B]) => Iterator[Z]) =
		new ComputeZippedPartitionsRDD2[A, B, Z](a, b, fct).send()

	def externalAppendOnlyMapInsertAll[K,V,C](
			entries2: SgxIteratorIdentifier[Product2[K, V]],
			mapId: SizeTrackingAppendOnlyMapIdentifier,
			mergeValue: (C, V) => C,
			createCombiner: V => C,
			depNum: Int = Integer.MIN_VALUE) =
		new ExternalAppendOnlyMapInsertAll[K,V,C](entries2, mapId, mergeValue, createCombiner, depNum).send()

	def externalAppendOnlyMapInsertAllCoGrouped[K](
			entries: SgxIteratorIdentifier[Encrypted],
			mapId: SizeTrackingAppendOnlyMapIdentifier,
			numDep: Int) = new externalAppendOnlyMapInsertAllCoGrouped[K](entries, mapId, numDep).send()
		
	def externalSorterInsertAllCombine[K,V,C](
			records: SgxIteratorIdentifier[Product2[K, V]],
			mapId: SizeTrackingAppendOnlyMapIdentifier,
			mergeValue: (C, V) => C,
			createCombiner: V => C,
			shouldPartition: Boolean,
			partitioner: Option[Partitioner]) =
		new ExternalSorterInsertAllCombine[K,V,C](records, mapId, mergeValue, createCombiner, shouldPartition, partitioner).send()

	def resultTaskRunTask[T,U](id: SgxIteratorIdentifier[T], func: (TaskContext, Iterator[T]) => U, context: TaskContext) =
		new ResultTaskRunTask[T,U](id, func, context).send()

	def resultTaskRunTaskAfterShuffle[T,U](id: SgxIteratorIdentifier[T], func: (TaskContext, Iterator[T]) => U, context: TaskContext) =
		new ResultTaskRunTaskAfterShuffle[T,U](id, func, context).send()
}

private case class ComputeMapPartitionsRDD[U, T](
	id: SgxIteratorIdentifier[T],
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int)
	extends SgxMessage[Iterator[U]] {

	def execute() = SgxFakeIterator(
		Await.result(Future {
			fct(partIndex, id.getIterator())		  
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

private case class ComputePartitionwiseSampledRDD[T, U](
	it: SgxIteratorIdentifier[T],
	sampler: RandomSampler[T, U]) extends SgxMessage[Iterator[U]] {

	def execute() = SgxFakeIterator(
		Await.result( Future {
			sampler.sample(it.getIterator())
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}

private case class ComputeZippedPartitionsRDD2[A, B, Z](
	a: SgxIteratorIdentifier[A],
	b: SgxIteratorIdentifier[B],
	fct: (Iterator[A], Iterator[B]) => Iterator[Z]) extends SgxMessage[Iterator[Z]] {

	def execute() = SgxFakeIterator(
		Await.result( Future {
			fct(a.getIterator(), b.getIterator())
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

private case class ExternalAppendOnlyMapInsertAll[K,V,C](
	entries2: SgxIteratorIdentifier[Product2[K, V]],
	mapId: SizeTrackingAppendOnlyMapIdentifier,
	mergeValue: (C, V) => C,
	createCombiner: V => C,
	depNum: Int) extends SgxMessage[Unit] {

	def execute() = Await.result(Future {
		val entries = 
			if (depNum == Integer.MIN_VALUE) {
				entries2.getIterator()
			}
			else {
				entries2.getIterator("cogroup").map(_._1.asInstanceOf[Encrypted].decrypt[Product2[K,Any]]).map(pair => (pair._1, (pair._2, depNum).asInstanceOf[V]))
			}

		val currentMap = mapId.getMap[K,C]
		var _peakMemoryUsedBytes = 0L

		var curEntry: Product2[K, V] = null
		val update: (Boolean, C) => C = (hadVal, oldVal) => {
			if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
		}

		while (entries.hasNext) {
			curEntry = entries.next()
			val estimatedSize = currentMap.estimateSize()
			if (estimatedSize > _peakMemoryUsedBytes) {
				_peakMemoryUsedBytes = estimatedSize
			}
			currentMap.changeValue(curEntry._1, update)
		}
	}, Duration.Inf)
}

private case class externalAppendOnlyMapInsertAllCoGrouped[K](
	entries: SgxIteratorIdentifier[Encrypted],
	mapId: SizeTrackingAppendOnlyMapIdentifier,
	numDep: Int) extends SgxMessage[Unit] {
 
	def execute() = Await.result(Future {
		mapId.getMap[K,Any]
	}, Duration.Inf)
}

private case class ExternalSorterInsertAllCombine[K,V,C](
	records2: SgxIteratorIdentifier[Product2[K, V]],
	mapId: SizeTrackingAppendOnlyMapIdentifier,
	mergeValue: (C, V) => C,
	createCombiner: V => C,
	shouldPartition: Boolean,
	partitioner: Option[Partitioner]) extends SgxMessage[Unit] {

	def execute() = Await.result(Future {
		val records = records2.getIterator()
		val map = mapId.getMap[K,C].asInstanceOf[PartitionedAppendOnlyMap[K,C]]
		var kv: Product2[K, V] = null
		val update = (hadValue: Boolean, oldValue: C) => {
			if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
		}
		while (records.hasNext) {
        	kv = records.next()
        	map.changeValue((if (shouldPartition) partitioner.get.getPartition(kv._1) else 0, kv._1), update)
		}
	}, Duration.Inf)//.asInstanceOf[U]
}

private case class ResultTaskRunTask[T,U](
	id: SgxIteratorIdentifier[T],
	func: (TaskContext, Iterator[T]) => U,
	context: TaskContext) extends SgxMessage[U] {

	def execute() = Await.result(Future {
		func(context, id.getIterator())
	}, Duration.Inf).asInstanceOf[U]

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", func=" + func + ", context=" + context + ")"
}

private case class ResultTaskRunTaskAfterShuffle[T,U](
	id: SgxIteratorIdentifier[T],
	func: (TaskContext, Iterator[T]) => U,
	context: TaskContext) extends SgxMessage[U] {

	def execute() = Await.result(Future {
		func(context, id.getIterator().asInstanceOf[Iterator[Product2[Encrypted,Any]]].map(_._1.decrypt[Product2[Product2[Any,Any],Any]]).map(c => (c._1._2, c._2)).asInstanceOf[Iterator[T]])
	}, Duration.Inf).asInstanceOf[U]

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", func=" + func + ", context=" + context + ")"
}
