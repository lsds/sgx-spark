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
import org.apache.spark.sgx.Decrypt
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

import org.apache.spark.sgx.EncryptionUtils._

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
