package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.random.RandomSampler

import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxIteratorIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

object SgxIteratorFct {
	def computePartitionwiseSampledRDD[T, U](it: SgxFakeIterator[T], sampler: RandomSampler[T, U]) = new SgxIteratorComputePartitionwiseSampledRDD[T, U](it, sampler).executeInsideEnclave()

	def computeMapPartitionsRDD[U, T](id: SgxIteratorIdentifier[T], fct: (Int, Iterator[T]) => Iterator[U], partIndex: Int) = new SgxIteratorComputeMapPartitionsRDD[U, T](id, fct, partIndex).executeInsideEnclave()

	def computeZippedPartitionsRDD2[A, B, Z](a: SgxFakeIterator[A], b: SgxFakeIterator[B], fct: (Iterator[A], Iterator[B]) => Iterator[Z]) = new SgxIteratorComputeZippedPartitionsRDD2[A, B, Z](a, b, fct).executeInsideEnclave()

	def fold[T](id: SgxIteratorProviderIdentifier[T], v: T, op: (T,T) => T) = new SgxIteratorFold(id, v, op).executeInsideEnclave()
}

private case class SgxIteratorFold[T](
	id: SgxIteratorProviderIdentifier[T],
	v: T,
	op: (T,T) => T) extends SgxExecuteInside[T] {

	def apply() = {
		Await.result(Future {
			new SgxIteratorConsumer[T](id).fold(v)(op)
			}, Duration.Inf).asInstanceOf[T]
	}
	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
}

private case class SgxIteratorComputeMapPartitionsRDD[U, T](
	id: SgxIteratorIdentifier[T],
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int)
	extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		val f = SgxFakeIterator()
		val g = Await.result(Future { fct(partIndex, id.getIterator) }, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		f
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

private case class SgxIteratorComputeZippedPartitionsRDD2[A, B, Z](
	a: SgxFakeIterator[A],
	b: SgxFakeIterator[B],
	fct: (Iterator[A], Iterator[B]) => Iterator[Z]) extends SgxExecuteInside[Iterator[Z]] {

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

private case class SgxIteratorComputePartitionwiseSampledRDD[T, U](
	it: SgxFakeIterator[T],
	sampler: RandomSampler[T, U]) extends SgxExecuteInside[Iterator[U]] {

	def apply() = {
		val f = SgxFakeIterator()
		val g = Await.result( Future { sampler.sample(SgxMain.fakeIterators.remove[Iterator[T]](it.id)) }, Duration.Inf)
		SgxMain.fakeIterators.put(f.id, g)
		f
	}

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}