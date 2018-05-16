package org.apache.spark.sgx.iterator

import java.util.concurrent.Callable

import scala.collection.mutable.ArrayBuffer

import scala.util.control.Breaks._

import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.lang3.SerializationException

import org.apache.spark.InterruptibleIterator
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.Encrypted
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.shm.ShmCommunicationManager

abstract class SgxIteratorProv[T] extends InterruptibleIterator[T](null, null) with SgxIterator[T] with Logging {
  def getIdentifier: SgxIteratorProvIdentifier[T]
}

class SgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean) extends SgxIteratorProv[T] with Callable[Unit] {

	private val com = ShmCommunicationManager.get().newShmCommunicator(false)

	private val identifier = new SgxIteratorProviderIdentifier[T](com.getMyPort)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])

	override final def hasNext: Boolean = delegate.hasNext

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the message interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override final def next(): T = throw new UnsupportedOperationException("Access this special Iterator via messages.")

	override def getIdentifier = identifier

	def call(): Unit = {
		val com = do_accept
		logDebug(this + " got connection: " + com)

		var running = true
		while (running) {
			com.recvOne() match {
				case num: MsgIteratorReqNextN => {
					val q = new ArrayBuffer[T](num.num)
					if (delegate.isInstanceOf[NextIterator[T]] && delegate.hasNext) {
						for (i <- 0 to num.num - 1 if delegate.hasNext) {
							val n = delegate.next
							// Need to clone the object. Otherwise the same object will be sent multiple times.
							// It seems that this is due to optimizations in the implementation of class NextIterator.
							// If cloning is not possible, just add this one object and send it individually.
							// This should never be the case, as the object _must_ be sent to a consumer.
						  q.insert(i, SerializationUtils.clone(n.asInstanceOf[Serializable]).asInstanceOf[T])
						}
					} else {
						for (i <- 0 to num.num - 1 if delegate.hasNext) {
							q.insert(i, delegate.next)
						}
					}
					val qe = if (doEncrypt) Encrypt(q) else q
					com.sendOne(qe)
				}
				case MsgIteratorReqClose =>
					stop(com)
					running = false

				case x: Any => logDebug(this + ": Unknown input message provided.")
			}
		}
	}

	def stop(com: SgxCommunicator) = {
		logDebug(this + ": Stopping ")
		com.close()
	}

	override def toString() = this.getClass.getSimpleName + "(identifier=" + identifier + ", com=" + com + ")"
}

class SgxShmIteratorProvider[T]() extends SgxIteratorProv[T] {
  
  private val identifier = new SgxShmIteratorProviderIdentifier[T](scala.util.Random.nextLong())

	override def getIdentifier = identifier
}