package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.random.RandomSampler

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

import org.apache.spark.deploy.SparkApplication
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import org.apache.spark.storage.StorageLevel

import java.lang.management.ManagementFactory

abstract class SgxExecuteInside[R] extends Serializable with Logging {
	def executeInsideEnclave(): R = {
		logDebug(this + ".executeInsideEnclave()");
		val x = ClientHandle.sendRecv[R](this)
		logDebug(this + ".executeInsideEnclave() returned: " + x);
		x
	}

	def apply(): R
}

case class SgxFirstTask[U: ClassTag, T: ClassTag](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
	extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		val f = SgxFakeIterator(scala.util.Random.nextLong)
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
		val f = SgxFakeIterator(scala.util.Random.nextLong)
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
		Await.result(Future { new SgxIteratorConsumer[T](id).fold(v)(op) }, Duration.Inf).asInstanceOf[T]
	}
	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
}

case class SgxComputeTaskZippedPartitionsRDD2[A, B, Z](
	fct: (Iterator[A], Iterator[B]) => Iterator[Z],
	a: SgxFakeIterator[A],
	b: SgxFakeIterator[B]) extends SgxExecuteInside[Iterator[Z]] {

	def apply() = {
		logDebug("apply")
		val f = SgxFakeIterator(scala.util.Random.nextLong)
		val g = Await.result( Future {
		  fct(SgxMain.fakeIterators.remove[Iterator[A]](a.id), SgxMain.fakeIterators.remove[Iterator[B]](b.id))
		}, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		logDebug("returning: " + f)
		f
	}

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxComputeTaskPartitionwiseSampledRDD[T, U](
	sampler: RandomSampler[T, U],
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		logDebug("apply")
		val f = SgxFakeIterator(scala.util.Random.nextLong)
		val g = Await.result( Future { sampler.sample(SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		logDebug("returning: " + f)
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

case class SgxTaskSparkContextDefaultParallelism() extends SgxExecuteInside[Int] {
	def apply() = Await.result( Future { SgxMain.sparkContext.defaultParallelism }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

case class SgxTaskSparkContextTextFile(path: String) extends SgxExecuteInside[RDD[String]] {

	def apply() = Await.result( Future {
		val rdd = SgxMain.sparkContext.textFile(path)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(path=" + path + ")"
}

case class SgxTaskSparkContextStop() extends SgxExecuteInside[Unit] {

	def apply() = Await.result( Future {
		SgxMain.sparkContext.stop()
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "()"
}

case class SgxTaskSparkContextNewRddId() extends SgxExecuteInside[Int] {

	def apply() = Await.result( Future {
		SgxMain.sparkContext.newRddId()
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "()"
}

case class SgxTaskRDDPartitions[T](rddId: Int) extends SgxExecuteInside[Array[Partition]] {
logDebug("create SgxTaskRDDPartitions")
	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].partitions
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

case class SgxTaskRDDMap[T,U:ClassTag](rddId: Int, f: T => U) extends SgxExecuteInside[RDD[U]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].map(f)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

case class SgxTaskRDDZip[T,U:ClassTag](rddId1: Int, rddId2: Int) extends SgxExecuteInside[RDD[(T,U)]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId1).asInstanceOf[RDD[T]].zip(SgxMain.rddIds.get(rddId2).asInstanceOf[RDD[U]])
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId1=" + rddId1 + ", rddId2=" + rddId2 + ")"
}

case class SgxTaskRDDPersist[T](rddId: Int, level: StorageLevel) extends SgxExecuteInside[RDD[T]] {

	def apply() = Await.result( Future {
		val r = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].persist(level)
		SgxMain.rddIds.put(r.id, r)
		r
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

case class SgxTaskRDDTakeSample[T](
	rddId: Int,
	withReplacement: Boolean,
    num: Int,
    seed: Long) extends SgxExecuteInside[Array[T]] {

	def apply() = Await.result( Future {
		logDebug("apply 1")
		val x = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]]
		logDebug("apply 2")
		val y = x.takeSample(withReplacement, num, seed)
		logDebug("apply 3")
		y
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

case class SgxTaskRDDUnpersist[T](rddId: Int) extends SgxExecuteInside[Unit] {

	def apply() = Await.result( Future {
		SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]].unpersist()
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(rddId=" + rddId + ")"
}

case class SgxTaskSparkContextRunJob[T, U: ClassTag](
	rddId: Int,
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit) extends SgxExecuteInside[Unit] {

	def apply() = SgxMain.sparkContext.runJob(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]], func, partitions, resultHandler)

	override def toString = this.getClass.getSimpleName + "()"
}

