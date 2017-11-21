package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

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

	def apply(): Iterator[U] = Await.result(Future { fct(partIndex, new SgxIteratorConsumer[T](id, false)) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", id=" + id + ")"
}

case class SgxOtherTask[U, T](
	fct: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: SgxFakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

	def apply(): Iterator[U] = Await.result(Future { fct(partIndex, SgxMain.fakeIterators.remove(it.id).asInstanceOf[Iterator[T]]) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + ", partIndex=" + partIndex + ", it=" + it + ")"
}

case class SgxFct2[A, B, Z](
	fct: (A, B) => Z,
	a: A,
	b: B) extends SgxExecuteInside[Z] {

	def apply(): Z = Await.result(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

case class SgxFct2Iterators[A, B, Z](
	fct: (Iterator[A], Iterator[B]) => Iterator[Z],
	a: SgxFakeIterator[A],
	b: SgxFakeIterator[B]) extends SgxExecuteInside[Iterator[Z]] {

//	def apply(): Z = Await.result(Future { fct(a, b) }, Duration.Inf)
	def apply(): Iterator[Z] = {
		logDebug("Executing " + this)
		val r = fct(SgxMain.fakeIterators.remove(a.id).asInstanceOf[Iterator[A]],SgxMain.fakeIterators.remove(b.id).asInstanceOf[Iterator[B]])
		logDebug(" result = " + r)
		r
	}
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}