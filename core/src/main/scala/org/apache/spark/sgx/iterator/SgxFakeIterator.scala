package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle

case class SgxFakeIterator[T](id: Long) extends Iterator[T] with Serializable {

	override def hasNext: Boolean =  throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")

	override def next: T = throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")

	def access(providerIsInEnclave: Boolean, key: Long = 0): Iterator[T] = {
		val iter = if (providerIsInEnclave) ClientHandle.sendRecv[SgxIteratorProviderIdentifier](MsgAccessFakeIterator(id))
			else ClientHandle.sendRecv[SgxIteratorProviderIdentifier](MsgAccessFakeIterator(id))

		new SgxIteratorConsumer(iter, providerIsInEnclave)
	}

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}