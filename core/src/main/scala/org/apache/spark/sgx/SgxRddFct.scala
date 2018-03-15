package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.mapred.JobConf

import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.OrderedRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier

object SgxRddFct {

	def collect[T](rddId: Int) =
		new Collect[T](rddId).send()

	def combineByKeyWithClassTag[C:ClassTag,V:ClassTag,K:ClassTag](
		rddId: Int,
		createCombiner: V => C,
		mergeValue: (C, V) => C,
		mergeCombiners: (C, C) => C,
		partitioner: Partitioner,
		mapSideCombine: Boolean,
		serializer: Serializer) =
			new CombineByKeyWithClassTag[C,V,K](rddId, createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer).send()

	def count[T](rddId: Int) =
		new Count[T](rddId).send()

	def filter[T](rddId: Int, f: T => Boolean) =
		new Filter(rddId, f).send()

	def flatMap[T,U: ClassTag](rddId: Int, f: T => TraversableOnce[U]) =
		new FlatMap(rddId, f).send()

	def fold[T](rddId: Int, v: T, op: (T,T) => T) =
		new Fold(rddId, v, op).send()

	def map[T,U:ClassTag](rddId: Int, f: T => U) =
		new Map(rddId, f).send()

	def mapPartitions[T,U:ClassTag](
		rddId: Int,
		f: Iterator[T] => Iterator[U],
		preservesPartitioning: Boolean) =
			new MapPartitions(rddId, f, preservesPartitioning).send()

	def mapPartitionsWithIndex[T,U:ClassTag](
		rddId: Int,
		f: (Int, Iterator[T]) => Iterator[U],
		preservesPartitioning: Boolean) =
			new MapPartitionsWithIndex(rddId, f, preservesPartitioning).send()

	def mapValues[U,V:ClassTag,K:ClassTag](rddId: Int, f: V => U) =
		new MapValues[U,V,K](rddId, f).send()

	def partitions[T](rddId: Int) =
		new Partitions[T](rddId).send()

	def persist[T](rddId: Int, level: StorageLevel) =
		new Persist[T](rddId, level).send()

	def sample[T](rddId: Int, withReplacement: Boolean, fraction: Double, seed: Long) =
		new Sample[T](rddId, withReplacement, fraction, seed).send()

	def saveAsTextFile[T](rddId: Int, path: String) =
		new SaveAsTextFile[T](rddId, path).send()

//	def sortBy[T,K](
//		f: (T) => K,
//		ascending: Boolean = true,
//		numPartitions: Int = this.partitions.length)
//		(implicit ord: Ordering[K], ctag: ClassTag[K]) =
//			new SortBy[T,K](f, ascending, numPartitions)(ord, ctag).send()

	def sortByKey[
			K : Ordering : ClassTag,
			V: ClassTag,
			P <: Product2[K, V] : ClassTag](rddId: Int, ascending: Boolean, numPartitions: Int) =
		new SortByKey[K,V,P](rddId, ascending, numPartitions).send()

	def unpersist[T](rddId: Int) =
		new Unpersist[T](rddId).send()

	def zip[T,U:ClassTag](rddId1: Int, rddId2: Int) =
		new Zip[T,U](rddId1, rddId2).send()
}

private abstract class SgxTaskRDD[T](val _rddId: Int) extends SgxMessage[T] {
	override def toString = this.getClass.getSimpleName + "(rddId=" + _rddId + ")"
}

private case class Collect[T](rddId: Int) extends SgxMessage[Array[T]] {
	def execute() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].collect()
	}, Duration.Inf)
}

private case class CombineByKeyWithClassTag[C:ClassTag,V:ClassTag,K:ClassTag](
      rddId: Int,
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean,
      serializer: Serializer) extends SgxTaskRDD[RDD[(K, C)]](rddId) {

	def execute() = Await.result( Future {
		val r = new PairRDDFunctions(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[(K, V)]]).combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class Count[T](rddId: Int) extends SgxTaskRDD[Long](rddId) {
	def execute() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].count()
	}, Duration.Inf)
}

private case class Filter[T](rddId: Int, f: T => Boolean) extends SgxTaskRDD[RDD[T]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].filter(f)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class FlatMap[T,U:ClassTag](rddId: Int, f: T => TraversableOnce[U]) extends SgxTaskRDD[RDD[U]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].flatMap(f)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class Fold[T](rddId: Int, v: T, op: (T,T) => T) extends SgxTaskRDD[T](rddId) {
	def execute() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].fold(v)(op)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", rddId=" + rddId + ")"
}

private case class Map[T,U:ClassTag](rddId: Int, f: T => U) extends SgxTaskRDD[RDD[U]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].map(f)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class MapPartitions[T,U:ClassTag](rddId: Int, f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean) extends SgxTaskRDD[RDD[U]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].mapPartitions(f)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class MapPartitionsWithIndex[T,U:ClassTag](rddId: Int, f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean) extends SgxTaskRDD[RDD[U]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].mapPartitionsWithIndex(f)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class MapValues[U,V:ClassTag,K:ClassTag](rddId: Int, f: V => U) extends SgxTaskRDD[RDD[(K, U)]](rddId) {
	def execute() = Await.result( Future {
		val r = new PairRDDFunctions(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[(K, V)]]).mapValues(f)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class Partitions[T](rddId: Int) extends SgxTaskRDD[Array[Partition]](rddId) {
	def execute() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].partitions
	}, Duration.Inf)
}

private case class Persist[T](rddId: Int, level: StorageLevel) extends SgxTaskRDD[RDD[T]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].persist(level)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class Sample[T](rddId: Int, withReplacement: Boolean, fraction: Double, seed: Long) extends SgxTaskRDD[RDD[T]](rddId) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].sample(withReplacement, fraction, seed)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class SaveAsTextFile[T](rddId: Int, path: String) extends SgxTaskRDD[Unit](rddId) {
	def execute() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].saveAsTextFile(path)
	}, Duration.Inf)
}

//private case class SortBy[T,K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length) (implicit ord: Ordering[K], ctag: ClassTag[K]) extends SgxTaskRDD[RDD[T]](rddId) {
//	def execute() = Await.result( Future {
//		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].sortBy(f, ascending, numPartitions)(ord, ctag)
//		SgxMain.rddIds.put(r.id, r)
//	}, Duration.Inf)
//}

private case class SortByKey[
		K : Ordering : ClassTag,
		V: ClassTag,
		P <: Product2[K, V] : ClassTag](rddId: Int, ascending: Boolean, numPartitions: Int) extends SgxTaskRDD[RDD[(K,V)]](rddId) {

	def execute() = Await.result( Future {
		val r = new OrderedRDDFunctions[K,V,P](SgxMain.rddIds.get(rddId).asInstanceOf[RDD[P]]).sortByKey(ascending, numPartitions)
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)
}

private case class Unpersist[T](rddId: Int) extends SgxTaskRDD[Unit](rddId) {
	def execute() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].unpersist()
	}, Duration.Inf)
}

private case class Zip[T,U:ClassTag](rddId1: Int, rddId2: Int) extends SgxTaskRDD[RDD[(T,U)]](-1) {
	def execute() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId1).asInstanceOf[RDD[T]].zip(SgxMain.rddIds.get(rddId2).asInstanceOf[RDD[U]])
		SgxMain.rddIds.put(r.id, r)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId1=" + rddId1 + ", rddId2=" + rddId2 + ")"
}
