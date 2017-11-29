package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.random.RandomSampler

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

class SgxExecuteInside[R] extends Serializable with Logging {
	def executeInsideEnclave(): R = {
		logDebug(this + ".executeInsideEnclave()");
		ClientHandle.sendRecv[R](this)
	}
}

case class SgxFirstTask[U: ClassTag, T: ClassTag](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
	extends SgxExecuteInside[Iterator[U]] {

	def apply(): Iterator[U] = {
		val x = SgxMain.fakeIterators.create(Await.result(Future { fct(partIndex, new SgxIteratorConsumer[T](id)) }, Duration.Inf))
		logDebug("SgxFirstTask in: "+id+", out: " + x.id)
		x.asInstanceOf[Iterator[U]]
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

case class SgxOtherTask[U, T](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply(): Iterator[U] = {
		val x = SgxMain.fakeIterators.create(Await.result(Future { fct(partIndex, SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf))
		logDebug("SgxOtherTask in: "+it.id+", out: " + x.id)
		x.asInstanceOf[Iterator[U]]
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", it=" + it + ")"
}

case class SgxFct2[A, B, Z](
	fct: (A, B) => Z,
	a: A,
	b: B) extends SgxExecuteInside[Z] {

	def apply(): Z = {
		val x = Await.result(Future { fct(a, b) }, Duration.Inf)
		logDebug("SgxFct2 in: ("+a+", "+b+"), out: " + x)
		x
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxComputeTaskZippedPartitionsRDD2[A, B, Z](
	fct: (Iterator[A], Iterator[B]) => Iterator[Z],
	a: SgxFakeIterator[A],
	b: SgxFakeIterator[B]) extends SgxExecuteInside[Iterator[Z]] {

	def apply(): Iterator[Z] = {
		val x = SgxMain.fakeIterators.create(Await.result( Future {
		  fct(SgxMain.fakeIterators.remove[Iterator[A]](a.id), SgxMain.fakeIterators.remove[Iterator[B]](b.id))
		}, Duration.Inf))
		logDebug("TaskZippedPartitionsRDD2 in: ("+a.id+", "+b.id+"), out: " + x.id)
		x.asInstanceOf[Iterator[Z]]
	}

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxComputeTaskPartitionwiseSampledRDD[T, U](
	sampler: RandomSampler[T, U],
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply(): Iterator[U] = {
		val x = SgxMain.fakeIterators.create(Await.result( Future {
		  sampler.sample(SgxMain.fakeIterators.remove[Iterator[T]](it.id))
		}, Duration.Inf))
		logDebug("PartitionwiseSampledRDD in: "+it.id+", out: " + x.id)
		x.asInstanceOf[Iterator[U]]
	}

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}
