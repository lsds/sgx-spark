package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.sgx.iterator.SgxIteratorConsumer
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier

object SgxIteratorFct {
	def fold[T](id: SgxIteratorProviderIdentifier, v: T, op: (T,T) => T) = new SgxIteratorFold(id, v, op).executeInsideEnclave()
}

private case class SgxIteratorFold[T](
	id: SgxIteratorProviderIdentifier,
	v: T,
	op: (T,T) => T) extends SgxExecuteInside[T] {

	def apply() = {
		Await.result(Future {
			new SgxIteratorConsumer[T](id).fold(v)(op)
			}, Duration.Inf).asInstanceOf[T]
	}
	override def toString = this.getClass.getSimpleName + "(v=" + v + " (" + v.getClass.getSimpleName + "), op=" + op + ", id=" + id + ")"
}