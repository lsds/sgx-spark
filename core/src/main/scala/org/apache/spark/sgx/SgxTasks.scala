package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.random.RandomSampler

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

import org.apache.spark.serializer.Serializer

import org.apache.spark.deploy.SparkApplication
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.AccumulatorMetadata

abstract class SgxExecuteInside[R] extends Serializable with Logging {
	def executeInsideEnclave(): R = {
		logDebug(this + ".executeInsideEnclave()");
		val x = ClientHandle.sendRecv[R](this)
		logDebug(this + ".executeInsideEnclave() returned: " + x);
		x
	}

	def apply(): R
}

object SgxRddFct {

	def collect[T](rddId: Int) = new SgxTaskRDDCollect[T](rddId).executeInsideEnclave()

	def combineByKeyWithClassTag[C:ClassTag,V:ClassTag,K:ClassTag](
		rddId: Int,
		createCombiner: V => C,
		mergeValue: (C, V) => C,
		mergeCombiners: (C, C) => C,
		partitioner: Partitioner,
		mapSideCombine: Boolean,
		serializer: Serializer) = new SgxTaskRDDCombineByKeyWithClassTag[C,V,K](rddId, createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer).executeInsideEnclave()

	def count[T](rddId: Int) = new SgxTaskRDDCount[T](rddId).executeInsideEnclave()

	def fold[T](rddId: Int, v: T, op: (T,T) => T) = new SgxTaskRDDFold(rddId, v, op).executeInsideEnclave()

	def map[T,U:ClassTag](rddId: Int, f: T => U) = new SgxTaskRDDMap(rddId, f).executeInsideEnclave()

	def mapPartitions[T,U:ClassTag](
		rddId: Int,
		f: Iterator[T] => Iterator[U],
		preservesPartitioning: Boolean) = new SgxTaskRDDMapPartitions(rddId, f, preservesPartitioning).executeInsideEnclave()

	def mapPartitionsWithIndex[T,U:ClassTag](
		rddId: Int,
		f: (Int, Iterator[T]) => Iterator[U],
		preservesPartitioning: Boolean) = new SgxTaskRDDMapPartitionsWithIndex(rddId, f, preservesPartitioning).executeInsideEnclave()

	def mapValues[U,V:ClassTag,K:ClassTag](rddId: Int, f: V => U) = new SgxTaskRDDMapValues[U,V,K](rddId, f).executeInsideEnclave()

	def persist[T](rddId: Int, level: StorageLevel) = new SgxTaskRDDPersist[T](rddId, level).executeInsideEnclave()

	def sample[T](rddId: Int, withReplacement: Boolean, fraction: Double, seed: Long) = new SgxTaskRDDSample[T](rddId, withReplacement, fraction, seed).executeInsideEnclave()

	def unpersist[T](rddId: Int) = new SgxTaskRDDUnpersist[T](rddId).executeInsideEnclave()

	def zip[T,U:ClassTag](rddId1: Int, rddId2: Int) = new SgxTaskRDDZip[T,U](rddId1, rddId2).executeInsideEnclave()
}

object SgxAccumulatorV2Fct {

	def register(
		acc: AccumulatorV2[_, _],
		name: Option[String] = None) = new SgxTaskAccumulatorRegister(acc, name).executeInsideEnclave()
}



case class SgxFirstTask[U: ClassTag, T: ClassTag](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
	extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		val f = SgxFakeIterator()
		val g = Await.result(Future { fct(partIndex, new SgxIteratorConsumer[T](id)) }, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		f
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

case class SgxOtherTask[U, T](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		val f = SgxFakeIterator()
		val g = Await.result(Future { fct(partIndex, SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		f
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", it=" + it + ")"
}

case class SgxFct0[Z](fct: () => Z) extends SgxExecuteInside[Z] {
	def apply() = Await.result(Future { fct() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

case class SgxFct2[A, B, Z](
	fct: (A, B) => Z,
	a: A,
	b: B) extends SgxExecuteInside[Z] {

	def apply() = Await.result(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxFold[T](
	v: T,
	op: (T,T) => T,
	id: SgxIteratorProviderIdentifier) extends SgxExecuteInside[T] {

	def apply() = {
		Await.result(Future {
			new SgxIteratorConsumer[T](id).fold(v)(op)
			}, Duration.Inf).asInstanceOf[T]
	}
	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
}

private case class SgxTaskRDDFold[T](
	rddId: Int,
	v: T,
	op: (T,T) => T) extends SgxExecuteInside[T] {

	def apply() = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].fold(v)(op)

	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", rddId=" + rddId + ")"
}


case class SgxComputeTaskZippedPartitionsRDD2[A, B, Z](
	fct: (Iterator[A], Iterator[B]) => Iterator[Z],
	a: SgxFakeIterator[A],
	b: SgxFakeIterator[B]) extends SgxExecuteInside[Iterator[Z]] {

	def apply() = {
		val f = SgxFakeIterator()
		val g = Await.result( Future {
		  fct(SgxMain.fakeIterators.remove[Iterator[A]](a.id), SgxMain.fakeIterators.remove[Iterator[B]](b.id))
		}, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		f
	}

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxComputeTaskPartitionwiseSampledRDD[T, U](
	sampler: RandomSampler[T, U],
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		val f = SgxFakeIterator()
		val g = Await.result( Future { sampler.sample(SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		f
	}

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}

case class SgxTaskExternalSorterInsertAllCreateKey[K](
	partitioner: Partitioner,
	pair: Product2[K,Any]) extends SgxExecuteInside[(Int,K)] {

	def apply() = Await.result( Future {
			(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
		}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
}

case class SgxTaskCreateSparkContext(conf: SparkConf) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.sparkContext = new SparkContext(conf); Unit }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(conf=" + conf + ")"
}

private case class SgxTaskAccumulatorRegister[T,U](
		acc: AccumulatorV2[T,U],
		name: Option[String]) extends SgxExecuteInside[AccumulatorMetadata] {

	def apply() = {
		if (name == null) acc.register(SgxMain.sparkContext)
		else acc.register(SgxMain.sparkContext, name)
		acc.metadata
	}

	override def toString = this.getClass.getSimpleName + "()"
}

case class SgxTaskRDDPartitions[T](rddId: Int) extends SgxExecuteInside[Array[Partition]] {

	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].partitions
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDMap[T,U:ClassTag](rddId: Int, f: T => U) extends SgxExecuteInside[RDD[U]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].map(f)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDMapValues[U,V:ClassTag,K:ClassTag](rddId: Int, f: V => U) extends SgxExecuteInside[RDD[(K, U)]] {

	def apply() = Await.result( Future {
		val r = new PairRDDFunctions(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[(K, V)]]).mapValues(f)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDCombineByKeyWithClassTag[C:ClassTag,V:ClassTag,K:ClassTag](
      rddId: Int,
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean,
      serializer: Serializer) extends SgxExecuteInside[RDD[(K, C)]] {

	def apply() = Await.result( Future {
		val r = new PairRDDFunctions(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[(K, V)]]).combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)
}

private case class SgxTaskRDDMapPartitionsWithIndex[T,U:ClassTag](rddId: Int, f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean) extends SgxExecuteInside[RDD[U]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].mapPartitionsWithIndex(f)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDMapPartitions[T,U:ClassTag](rddId: Int, f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean) extends SgxExecuteInside[RDD[U]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].mapPartitions(f)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDZip[T,U:ClassTag](rddId1: Int, rddId2: Int) extends SgxExecuteInside[RDD[(T,U)]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId1).asInstanceOf[RDD[T]].zip(SgxMain.rddIds.get(rddId2).asInstanceOf[RDD[U]])
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId1=" + rddId1 + ", rddId2=" + rddId2 + ")"
}

private case class SgxTaskRDDPersist[T](rddId: Int, level: StorageLevel) extends SgxExecuteInside[RDD[T]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].persist(level)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDSample[T](
	rddId: Int,
	withReplacement: Boolean,
    fraction: Double,
    seed: Long) extends SgxExecuteInside[RDD[T]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].sample(withReplacement, fraction, seed)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDCollect[T](
	rddId: Int) extends SgxExecuteInside[Array[T]] {

	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].collect()
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDCount[T](
	rddId: Int) extends SgxExecuteInside[Long] {

	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].count()
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDUnpersist[T](rddId: Int) extends SgxExecuteInside[Unit] {

	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].unpersist()
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

