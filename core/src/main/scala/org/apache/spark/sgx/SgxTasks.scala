package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.random.RandomSampler

import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

import org.apache.spark.deploy.SparkApplication
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.lang.management.ManagementFactory

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

case class SgxFct0[Z](
	fct: () => Z) extends SgxExecuteInside[Z] {

	def apply() = {
		Await.result(Future { fct() }, Duration.Inf)
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

case class SgxFct2[A, B, Z](
	fct: (A, B) => Z,
	a: A,
	b: B) extends SgxExecuteInside[Z] {

	def apply() = {
		logDebug("apply()")
		Await.result(Future { fct(a, b) }, Duration.Inf)
	}
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

//case class SgxTaskGetPartitionSgxProduct2Key(
//	partitioner: Partitioner,
//	pair: Product2[Any,Any]) extends SgxExecuteInside[Int] {
//
//	def apply() = Await.result( Future { partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1) }, Duration.Inf)
//
//	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
//}

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

case class SgxTaskCreateSparkContext[K](conf: SparkConf) extends SgxExecuteInside[Unit] {

	def apply() = {
		Await.result( Future {
			SgxMain.sparkContext = new SparkContext(conf)
			Unit
		}, Duration.Inf)
	}

	override def toString = this.getClass.getSimpleName + "(conf=" + conf + ")"
}


//case class SgxTaskStartApp(
//	app: SparkApplication,
//	args: Array[String],
//	conf: SparkConf) extends SgxExecuteInside[Unit] {
//
//	def apply() = {
//		Await.result( Future {
//			(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
//		}, Duration.Inf)
//	}
//
//	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
//}
