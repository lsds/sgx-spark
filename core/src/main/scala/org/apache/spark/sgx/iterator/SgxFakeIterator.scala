package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle
import org.apache.spark.sgx.IdentifierManager
import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.SgxFct

import org.apache.spark.util.collection.WritablePartitionedIterator
import org.apache.spark.storage.DiskBlockObjectWriter

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

private object WritablePartitionedFakeIterators {
	val map = new IdentifierManager[WritablePartitionedIterator]()
}

case class SgxWritablePartitionedFakeIterator(@transient val delegate: WritablePartitionedIterator) extends WritablePartitionedIterator with Logging {

	val id = scala.util.Random.nextLong

	WritablePartitionedFakeIterators.map.put(id, delegate)

	override def writeNext(writer: DiskBlockObjectWriter) = {
		if (SgxSettings.IS_DRIVER) {
			logDebug("writeNext DRIVER")
//			delegate.writeNext(writer)
		}
		else if (SgxSettings.IS_WORKER) {
			logDebug("writeNext WORKER")
//			SgxFct.writablePartitionedIteratorWriteNext(this)
		}
		else {
			logDebug("writeNext MISC")
			throw SgxFakeIteratorException(id)
		}
	}

	override def hasNext: Boolean = {
		if (SgxSettings.IS_DRIVER) {
			logDebug("hasNext DRIVER")
			delegate.hasNext()
		}
		else if (SgxSettings.IS_WORKER) {
			logDebug("hasNext WORKER")
			SgxFct.writablePartitionedIteratorHasNext(this)
		}
		else {
			logDebug("hasNext MISC")
			throw SgxFakeIteratorException(id)
		}
	}

	override def nextPartition() = {
		if (SgxSettings.IS_DRIVER) {
			logDebug("nextPartition DRIVER")
			delegate.nextPartition()
		}
		else if (SgxSettings.IS_WORKER) {
			logDebug("nextPartition WORKER")
			SgxFct.writablePartitionedIteratorNextPartition(this)
		}
		else {
			logDebug("nextPartition MISC")
			throw SgxFakeIteratorException(id)
		}
	}

	def getIterator = WritablePartitionedFakeIterators.map.get(id)
}