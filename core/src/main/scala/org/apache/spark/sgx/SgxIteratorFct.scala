package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.random.RandomSampler
import org.apache.spark.util.collection.PartitionedAppendOnlyMap
import org.apache.spark.util.collection.PartitionedAppendOnlyMapIdentifier

import org.apache.spark.Partitioner
import org.apache.spark.TaskContext
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxIteratorIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

object SgxIteratorFct {
	def computeMapPartitionsRDD[U, T](id: SgxIteratorIdentifier[T], fct: (Int, Iterator[T]) => Iterator[U], partIndex: Int) =
		new ComputeMapPartitionsRDD[U, T](id, fct, partIndex).send()

	def computePartitionwiseSampledRDD[T, U](it: SgxIteratorIdentifier[T], sampler: RandomSampler[T, U]) =
		new ComputePartitionwiseSampledRDD[T, U](it, sampler).send()

	def computeZippedPartitionsRDD2[A, B, Z](a: SgxIteratorIdentifier[A], b: SgxIteratorIdentifier[B], fct: (Iterator[A], Iterator[B]) => Iterator[Z]) =
		new ComputeZippedPartitionsRDD2[A, B, Z](a, b, fct).send()

	def resultTaskRunTask[T,U](id: SgxIteratorIdentifier[T], func: (TaskContext, Iterator[T]) => U, context: TaskContext) =
		new ResultTaskRunTask[T,U](id, func, context).send()

	def resultTaskRunTaskAfterShuffle[T,U](id: SgxIteratorIdentifier[T], func: (TaskContext, Iterator[T]) => U, context: TaskContext) =
		new ResultTaskRunTaskAfterShuffle[T,U](id, func, context).send()

	def externalSorterInsertAllCombine[K,V,C](
			records: SgxIteratorIdentifier[Product2[K, V]],
			mapId: PartitionedAppendOnlyMapIdentifier,
			mergeValue: (C, V) => C,
			createCombiner: V => C,
			shouldPartition: Boolean,
			partitioner: Option[Partitioner]) =
		new ExternalSorterInsertAllCombine[K,V,C](records, mapId, mergeValue, createCombiner, shouldPartition, partitioner).send()
}

private case class ComputeMapPartitionsRDD[U, T](
	id: SgxIteratorIdentifier[T],
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int)
	extends SgxMessage[Iterator[U]] {

	def execute() = SgxFakeIterator(
		Await.result(Future {
			fct(partIndex, id.getIterator)
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

private case class ComputePartitionwiseSampledRDD[T, U](
	it: SgxIteratorIdentifier[T],
	sampler: RandomSampler[T, U]) extends SgxMessage[Iterator[U]] {

	def execute() = SgxFakeIterator(
		Await.result( Future {
			sampler.sample(it.getIterator)
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
			fct(a.getIterator, b.getIterator)
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

private case class ResultTaskRunTask[T,U](
	id: SgxIteratorIdentifier[T],
	func: (TaskContext, Iterator[T]) => U,
	context: TaskContext) extends SgxMessage[U] {

	def execute() = Await.result(Future {
		func(context, id.getIterator)
	}, Duration.Inf).asInstanceOf[U]

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", func=" + func + ", context=" + context + ")"
}

private case class ResultTaskRunTaskAfterShuffle[T,U](
	id: SgxIteratorIdentifier[T],
	func: (TaskContext, Iterator[T]) => U,
	context: TaskContext) extends SgxMessage[U] {

	def execute() = Await.result(Future {
//		id.getIterator.foreach(x => logDebug("xxxx " + x.asInstanceOf[Pair[Encrypted,Any]]._1.decrypt))
//		id.getIterator.asInstanceOf[Iteratator[Pair[Encrypted,Any]]].map(_._1.decrypt)
		func(context, id.getIterator.asInstanceOf[Iterator[Pair[Encrypted,Any]]].map(_._1.decrypt[Pair[Pair[Any,Any],Any]]).map(c => (c._1._2, c._2)).asInstanceOf[Iterator[T]])
	}, Duration.Inf).asInstanceOf[U]

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", func=" + func + ", context=" + context + ")"
}

private case class ExternalSorterInsertAllCombine[K,V,C](
	records2: SgxIteratorIdentifier[Product2[K, V]],
	mapId: PartitionedAppendOnlyMapIdentifier,
	mergeValue: (C, V) => C,
	createCombiner: V => C,
	shouldPartition: Boolean,
	partitioner: Option[Partitioner]) extends SgxMessage[Unit] {

	def execute() = Await.result(Future {
		val records = records2.getIterator
		val map = mapId.getMap[K,C]
		logDebug("ExternalSorterInsertAllCombine: " + records2 + ", " + mapId)
		var kv: Product2[K, V] = null
		val update = (hadValue: Boolean, oldValue: C) => {
			if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
		}
		while (records.hasNext) {
//			addElementsRead() // make ocall
        	kv = records.next()
        	map.changeValue((if (shouldPartition) partitioner.get.getPartition(kv._1) else 0, kv._1), update)
			logDebug("ExternalSorterInsertAllCombine: changeValue("+(if (shouldPartition) partitioner.get.getPartition(kv._1) else 0)+","+kv._1+") to " + update)
//			maybeSpillCollection(usingMap = true) // make ocall
		}
	}, Duration.Inf)//.asInstanceOf[U]
}
