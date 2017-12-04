package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle

case class SgxFakeIteratorException(id: Long) extends RuntimeException("A FakeIterator is just a placeholder and not supposed to be used. (" + id + ")") {}

case class SgxFakeIterator[T](id: Long) extends Iterator[T] with Serializable {

	override def hasNext: Boolean =  throw SgxFakeIteratorException(id)
	override def next: T = throw SgxFakeIteratorException(id)

	def access(providerIsInEnclave: Boolean): Iterator[T] = {
		val iter = if (providerIsInEnclave) ClientHandle.sendRecv[SgxIteratorProviderIdentifier](MsgAccessFakeIterator(id))
			else ClientHandle.sendRecv[SgxIteratorProviderIdentifier](MsgAccessFakeIterator(id))

		new SgxIteratorConsumer(iter)
	}

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}