package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.random.RandomSampler

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.Aggregator
import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.serializer.Serializer
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

import java.lang.management.ManagementFactory
import org.apache.spark.TaskContext

abstract class SgxExecuteInside[R] extends Serializable with Logging {
	def executeInsideEnclave(): R = {
		logDebug(this + ".executeInsideEnclave()");
		ClientHandle.sendRecv[R](this)
	}

	def apply(): R
}

case class SgxFirstTask[U: ClassTag, T: ClassTag](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
	extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		SgxMain.fakeIterators.create(Await.result(Future { fct(partIndex, new SgxIteratorConsumer[T](id)) }, Duration.Inf)).asInstanceOf[Iterator[U]]
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

case class SgxOtherTask[U, T](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		SgxMain.fakeIterators.create(Await.result(Future { fct(partIndex, SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf)).asInstanceOf[Iterator[U]]
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", it=" + it + ")"
}

case class SgxAction[U, T](
	fct: Iterator[T] => U,
	it: SgxFakeIterator[T]) extends SgxExecuteInside[U] {

	def apply() = {
		Await.result(Future { fct(SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf)
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", it=" + it + ")"
}


case class SgxFct2[A, B, Z](
	fct: (A, B) => Z,
	a: Any,
	b: Any) extends SgxExecuteInside[Encrypted] {

	logDebug(this.toString())

	def apply() = {
		Await.result(Future { val x = fct(Decrypt[A](a), Decrypt[B](b)); logDebug("result: " + x); Encrypt(x) }, Duration.Inf)
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + " (" + (if (a != null) a.getClass.getName else "") + "), b=" + b + " (" + (if (b != null) b.getClass.getName else "") + "))"
}

case class SgxFold[T](
	v: T,
	op: (T,T) => T,
	id: SgxIteratorProviderIdentifier) extends SgxExecuteInside[T] {

	def apply() = {
		Await.result(Future { new SgxIteratorConsumer[T](id).fold(v)(op) }, Duration.Inf).asInstanceOf[T]
	}
	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
}

case class SgxComputeTaskZippedPartitionsRDD2[A, B, Z](
	fct: (Iterator[A], Iterator[B]) => Iterator[Z],
	a: SgxFakeIterator[A],
	b: SgxFakeIterator[B]) extends SgxExecuteInside[Iterator[Z]] {

	def apply() = {
		SgxMain.fakeIterators.create(Await.result( Future {
		  fct(SgxMain.fakeIterators.remove[Iterator[A]](a.id), SgxMain.fakeIterators.remove[Iterator[B]](b.id))
		}, Duration.Inf)).asInstanceOf[Iterator[Z]]
	}

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxComputeTaskPartitionwiseSampledRDD[T, U](
	sampler: RandomSampler[T, U],
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		SgxMain.fakeIterators.create(Await.result( Future {
		  sampler.sample(SgxMain.fakeIterators.remove[Iterator[T]](it.id))
		}, Duration.Inf)).asInstanceOf[Iterator[U]]
	}

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}

case class SgxTaskExternalSorterInsertAllCreateKey[K](
	partitioner: Partitioner,
	pair: Product2[K,Any]) extends SgxExecuteInside[(Int,K)] {

	def apply() = {
		Await.result( Future {
			(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
		}, Duration.Inf)
	}

	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
}

case class SgxTaskShuffledRDDCreate[K: ClassTag, V: ClassTag, C: ClassTag](
	@transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner) extends SgxExecuteInside[Long] {

	def apply() = {
		val s = Await.result( Future { new ShuffledRDD[K,V,C](prev, part) }, Duration.Inf)
		val id = scala.util.Random.nextLong()
		SgxMain.shuffledRdds.put(id, s)
		id
	}

	override def toString = this.getClass.getSimpleName + "(prev=" + prev + ", part=" + part + ")"
}

case class SgxTaskShuffledRDDSetSerializer(id: Long, serializer: Serializer) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[Any,Any,Any](id).setSerializer(serializer) }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", serializer=" + serializer + ")"
}

case class SgxTaskShuffledRDDSetKeyOrdering[K](id: Long, keyOrdering: Ordering[K]) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[K,Any,Any](id).setKeyOrdering(keyOrdering) }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", keyOrdering=" + keyOrdering + ")"
}

case class SgxTaskShuffledRDDSetAggregator[K,V,C](id: Long, aggregator: Aggregator[K,V,C]) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[K,V,C](id).setAggregator(aggregator) }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", aggregator=" + aggregator + ")"
}

case class SgxTaskShuffledRDDSetMapSideCombine(id: Long, mapSideCombine: Boolean) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[Any,Any,Any](id).setMapSideCombine(mapSideCombine) }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", mapSideCombine=" + mapSideCombine + ")"
}

case class SgxTaskShuffledRDDClearDependencies(id: Long) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[Any,Any,Any](id).clearDependencies() }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}

case class SgxTaskShuffledRDDGetPartitions(id: Long) extends SgxExecuteInside[Array[Partition]] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[Any,Any,Any](id).getPartitions }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}

case class SgxTaskShuffledRDDGetDependencies(id: Long) extends SgxExecuteInside[Seq[Dependency[_]]] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[Any,Any,Any](id).getDependencies }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}

//case class SgxTaskShuffledRDDGetPreferredLocations(id: Long, partition: Partition) extends SgxExecuteInside[Seq[String]] {
//	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[Any,Any,Any](id).getPreferredLocations(partition) }, Duration.Inf)
//
//	override def toString = this.getClass.getSimpleName + "(id=" + id + ", partition=" + partition + ")"
//}

case class SgxTaskShuffledRDDCompute[K,C](id: Long, split: Partition, context: TaskContext) extends SgxExecuteInside[Iterator[(K, C)]] {
	def apply() = Await.result( Future { SgxMain.shuffledRdds.get[K,Any,C](id).compute(split, context) }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", split=" + split + ", context=" + context + ")"
}
