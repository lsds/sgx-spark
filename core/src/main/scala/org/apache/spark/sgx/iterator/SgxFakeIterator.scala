package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle
import org.apache.spark.sgx.IdentifierManager
import org.apache.spark.sgx.SgxFactory

import org.apache.spark.internal.Logging

case class SgxFakeIteratorException(id: Long) extends RuntimeException("A FakeIterator is just a placeholder and not supposed to be used. (" + id + ")") {}

private object FakeIterators {
	val map = new IdentifierManager[Iterator[Any]]()
}

case class SgxFakeIterator[T](@transient val delegate: Iterator[T]) extends Iterator[T] with SgxIterator[T] with SgxIteratorIdentifier[T] with Logging {

	val id = scala.util.Random.nextLong

	FakeIterators.map.put(id, delegate)

	override def hasNext: Boolean = throw SgxFakeIteratorException(id)

	override def next: T = throw SgxFakeIteratorException(id)

	def access(): Iterator[T] = new SgxIteratorConsumer(ClientHandle.sendRecv[SgxIteratorProviderIdentifier[T]](MsgAccessFakeIterator(this)))

	def provide() = SgxFactory.newSgxIteratorProvider[Any](FakeIterators.map.remove(id), true).getIdentifier

	override def getIdentifier = this

	override def getIterator = FakeIterators.map.remove[Iterator[T]](id)

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}