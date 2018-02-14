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
	def computeMapPartitionsRDD[U, T](id: SgxIteratorIdentifier[T], fct: (Int, Iterator[T]) => Iterator[U], partIndex: Int) =
		new SgxIteratorComputeMapPartitionsRDD[U, T](id, fct, partIndex).send()

	def computePartitionwiseSampledRDD[T, U](it: SgxIteratorIdentifier[T], sampler: RandomSampler[T, U]) =
		new SgxIteratorComputePartitionwiseSampledRDD[T, U](it, sampler).send()

	def computeZippedPartitionsRDD2[A, B, Z](a: SgxIteratorIdentifier[A], b: SgxIteratorIdentifier[B], fct: (Iterator[A], Iterator[B]) => Iterator[Z]) =
		new SgxIteratorComputeZippedPartitionsRDD2[A, B, Z](a, b, fct).send()

	def fold[T](id: SgxIteratorIdentifier[T], v: T, op: (T,T) => T) =
		new SgxIteratorFold(id, v, op).send()
}

private case class SgxIteratorComputeMapPartitionsRDD[U, T](
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

private case class SgxIteratorComputePartitionwiseSampledRDD[T, U](
	it: SgxIteratorIdentifier[T],
	sampler: RandomSampler[T, U]) extends SgxMessage[Iterator[U]] {

	def execute() = SgxFakeIterator(
		Await.result( Future {
			sampler.sample(it.getIterator)
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}

private case class SgxIteratorComputeZippedPartitionsRDD2[A, B, Z](
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

private case class SgxIteratorFold[T](
	id: SgxIteratorIdentifier[T],
	v: T,
	op: (T,T) => T) extends SgxMessage[T] {

	def execute() = Await.result(Future {
		id.getIterator.fold(v)(op)
	}, Duration.Inf).asInstanceOf[T]

	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
}