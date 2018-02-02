package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle

case class SgxFakeIteratorException(id: Long) extends RuntimeException("A FakeIterator is just a placeholder and not supposed to be used. (" + id + ")") {}

case class SgxFakeIterator[T]() extends Iterator[T] with Serializable {

	val id = scala.util.Random.nextLong

	override def hasNext: Boolean =  throw SgxFakeIteratorException(id)
	override def next: T = throw SgxFakeIteratorException(id)

	def access(): Iterator[T] =
		new SgxIteratorConsumer(ClientHandle.sendRecv[SgxIteratorProviderIdentifier](MsgAccessFakeIterator(id)))

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}