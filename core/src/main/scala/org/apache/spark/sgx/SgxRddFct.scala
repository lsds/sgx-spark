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
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier

object SgxRddFct {

	def collect[T](rddId: Int) =
		new SgxTaskRDDCollect[T](rddId).executeInsideEnclave()

	def combineByKeyWithClassTag[C:ClassTag,V:ClassTag,K:ClassTag](
		rddId: Int,
		createCombiner: V => C,
		mergeValue: (C, V) => C,
		mergeCombiners: (C, C) => C,
		partitioner: Partitioner,
		mapSideCombine: Boolean,
		serializer: Serializer) =
			new SgxTaskRDDCombineByKeyWithClassTag[C,V,K](rddId, createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer).executeInsideEnclave()

	def count[T](rddId: Int) =
		new SgxTaskRDDCount[T](rddId).executeInsideEnclave()

	def flatMap[T,U: ClassTag](rddId: Int, f: T => TraversableOnce[U]) =
		new SgxTaskRDDFlatMap(rddId, f).executeInsideEnclave()

	def fold[T](rddId: Int, v: T, op: (T,T) => T) =
		new SgxTaskRDDFold(rddId, v, op).executeInsideEnclave()

	def map[T,U:ClassTag](rddId: Int, f: T => U) =
		new SgxTaskRDDMap(rddId, f).executeInsideEnclave()

	def mapPartitions[T,U:ClassTag](
		rddId: Int,
		f: Iterator[T] => Iterator[U],
		preservesPartitioning: Boolean) =
			new SgxTaskRDDMapPartitions(rddId, f, preservesPartitioning).executeInsideEnclave()

	def mapPartitionsWithIndex[T,U:ClassTag](
		rddId: Int,
		f: (Int, Iterator[T]) => Iterator[U],
		preservesPartitioning: Boolean) =
			new SgxTaskRDDMapPartitionsWithIndex(rddId, f, preservesPartitioning).executeInsideEnclave()

	def mapValues[U,V:ClassTag,K:ClassTag](rddId: Int, f: V => U) =
		new SgxTaskRDDMapValues[U,V,K](rddId, f).executeInsideEnclave()

	def partitions[T](rddId: Int) =
		new SgxTaskRDDPartitions[T](rddId).executeInsideEnclave()

	def persist[T](rddId: Int, level: StorageLevel) =
		new SgxTaskRDDPersist[T](rddId, level).executeInsideEnclave()

	def sample[T](rddId: Int, withReplacement: Boolean, fraction: Double, seed: Long) =
		new SgxTaskRDDSample[T](rddId, withReplacement, fraction, seed).executeInsideEnclave()

	def saveAsHadoopDataset[V:ClassTag,K:ClassTag](rddId: Int, conf: JobConf) =
		new SgxTaskRDDSaveAsHadoopDataset[V,K](rddId, conf).executeInsideEnclave()

	def saveAsTextFile[T](rddId: Int, path: String) =
		new SgxTaskRDDSaveAsTextFile[T](rddId, path).executeInsideEnclave()

	def unpersist[T](rddId: Int) =
		new SgxTaskRDDUnpersist[T](rddId).executeInsideEnclave()

	def zip[T,U:ClassTag](rddId1: Int, rddId2: Int) =
		new SgxTaskRDDZip[T,U](rddId1, rddId2).executeInsideEnclave()
}

private case class SgxTaskRDDCollect[T](rddId: Int) extends SgxExecuteInside[Array[T]] {
	def apply() = Await.result( Future { SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].collect() }, Duration.Inf)
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

private case class SgxTaskRDDCount[T](rddId: Int) extends SgxExecuteInside[Long] {
	def apply() = Await.result( Future { SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].count()}, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDFlatMap[T,U:ClassTag](rddId: Int, f: T => TraversableOnce[U]) extends SgxExecuteInside[RDD[U]] {
	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].flatMap(f)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDFold[T](rddId: Int, v: T, op: (T,T) => T) extends SgxExecuteInside[T] {
	def apply() = Await.result( Future { SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].fold(v)(op) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", rddId=" + rddId + ")"
}

private case class SgxTaskRDDMap[T,U:ClassTag](rddId: Int, f: T => U) extends SgxExecuteInside[RDD[U]] {
	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].map(f)
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

private case class SgxTaskRDDMapPartitionsWithIndex[T,U:ClassTag](rddId: Int, f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean) extends SgxExecuteInside[RDD[U]] {
	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].mapPartitionsWithIndex(f)
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

private case class SgxTaskRDDPartitions[T](rddId: Int) extends SgxExecuteInside[Array[Partition]] {
	def apply() = Await.result( Future { SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].partitions }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDPersist[T](rddId: Int, level: StorageLevel) extends SgxExecuteInside[RDD[T]] {
	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].persist(level)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDSample[T](rddId: Int, withReplacement: Boolean, fraction: Double, seed: Long) extends SgxExecuteInside[RDD[T]] {
	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].sample(withReplacement, fraction, seed)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDSaveAsHadoopDataset[V:ClassTag,K:ClassTag](rddId: Int, conf: JobConf) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future {
		new PairRDDFunctions(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[(K, V)]]).saveAsHadoopDataset(conf)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDSaveAsTextFile[T](rddId: Int, path: String) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].saveAsTextFile(path)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

private case class SgxTaskRDDUnpersist[T](rddId: Int) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].unpersist() }, Duration.Inf)
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
