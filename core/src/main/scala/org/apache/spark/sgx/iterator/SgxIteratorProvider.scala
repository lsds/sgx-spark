package org.apache.spark.sgx.iterator

import java.util.concurrent.Callable

import scala.collection.mutable.Queue
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

class SgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean) extends InterruptibleIterator[T](null, null) with SgxIterator[T] with Callable[Unit] with Logging {

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
					val q = Queue[T]()
					if (delegate.isInstanceOf[NextIterator[T]] && delegate.hasNext) {
						for (_ <- 1 to num.num if delegate.hasNext) {
							val n = delegate.next
							// Need to clone the object. Otherwise the same object will be sent multiple times.
							// It seems that this is due to optimizations in the implementation of class NextIterator.
							// If cloning is not possible, just add this one object and send it individually.
							// This should actually never be the case, as the object _must_ be sent to a consumer.
							try {
								q += SerializationUtils.clone(n.asInstanceOf[Serializable]).asInstanceOf[T]
							}
							catch {
								case e: SerializationException =>
									q += n
									break
							}
						}
					} else {
						for (_ <- 1 to num.num if delegate.hasNext) {
							q += delegate.next
						}
					}
					val qe = if (doEncrypt) q.map { n => Encrypt(n) } else q
					logDebug("Sending: " + qe)
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
