package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.random.RandomSampler

import org.apache.spark.TaskContext
import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.iterator.SgxIteratorIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator

object SgxIteratorFct {
	def computeMapPartitionsRDD[U, T](id: SgxIteratorIdentifier[T], fct: (Int, Iterator[T]) => Iterator[U], partIndex: Int) =
		new ComputeMapPartitionsRDD[U, T](id, fct, partIndex).send()

	def computePartitionwiseSampledRDD[T, U](it: SgxIteratorIdentifier[T], sampler: RandomSampler[T, U]) =
		new ComputePartitionwiseSampledRDD[T, U](it, sampler).send()

	def computeZippedPartitionsRDD2[A, B, Z](a: SgxIteratorIdentifier[A], b: SgxIteratorIdentifier[B], fct: (Iterator[A], Iterator[B]) => Iterator[Z]) =
		new ComputeZippedPartitionsRDD2[A, B, Z](a, b, fct).send()

//	def fold[T](id: SgxIteratorIdentifier[T], v: T, op: (T,T) => T) =
//		new ItFold[T](id, v, op).send()

	def runTaskFunc[T,U](id: SgxIteratorIdentifier[T], func: (TaskContext, Iterator[T]) => U, context: TaskContext) =
		new RunTaskFunc[T,U](id, func, context).send()
}

private case class ComputeMapPartitionsRDD[U, T](
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

private case class ComputePartitionwiseSampledRDD[T, U](
	it: SgxIteratorIdentifier[T],
	sampler: RandomSampler[T, U]) extends SgxMessage[Iterator[U]] {

	def execute() = SgxFakeIterator(
		Await.result( Future {
			sampler.sample(it.getIterator)
		}, Duration.Inf)
	)

	override def toString = this.getClass.getSimpleName + "(sampler=" + sampler + ", it=" + it + ")"
}

private case class ComputeZippedPartitionsRDD2[A, B, Z](
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

//private case class ItFold[T](
//	id: SgxIteratorIdentifier[T],
//	v: T,
//	op: (T,T) => T) extends SgxMessage[T] {
//
//	def execute() = Await.result(Future {
//		id.getIterator.fold(v)(op)
//	}, Duration.Inf).asInstanceOf[T]
//
//	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
//}

private case class RunTaskFunc[T,U](
	id: SgxIteratorIdentifier[T],
	func: (TaskContext, Iterator[T]) => U,
	context: TaskContext) extends SgxMessage[U] {

	def execute() = Await.result(Future {
		func(context, id.getIterator)
	}, Duration.Inf).asInstanceOf[U]

	override def toString = this.getClass.getSimpleName + "(id=" + id + ", func=" + func + ", context=" + context + ")"
}

