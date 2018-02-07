package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle
import org.apache.spark.sgx.SgxMain

case class SgxFakeIteratorException(id: Long) extends RuntimeException("A FakeIterator is just a placeholder and not supposed to be used. (" + id + ")") {}

case class SgxFakeIterator[T]() extends Iterator[T] with SgxIterator[T] with SgxIteratorIdentifier[T] {

	val id = scala.util.Random.nextLong

	override def hasNext: Boolean =  throw SgxFakeIteratorException(id)
	override def next: T = throw SgxFakeIteratorException(id)

	def access(): Iterator[T] =
		new SgxIteratorConsumer(ClientHandle.sendRecv[SgxIteratorProviderIdentifier[T]](MsgAccessFakeIterator(id)))

	override def getIdentifier = this

	override def getIterator = SgxMain.fakeIterators.remove[Iterator[T]](id)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}