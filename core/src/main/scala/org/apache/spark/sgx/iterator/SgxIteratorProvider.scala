package org.apache.spark.sgx.iterator

import java.util.concurrent.Callable

import scala.collection.mutable.Queue

import org.apache.spark.InterruptibleIterator
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.Encrypted
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings

abstract class SgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean) extends InterruptibleIterator[T](null, null) with Callable[Unit] with Logging {
	protected def do_accept: SgxCommunicator

	val identifier: SgxIteratorProviderIdentifier

	override final def hasNext: Boolean = delegate.hasNext

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the message interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override final def next(): T = throw new UnsupportedOperationException(s"Access this special Iterator via messages.")

	def call(): Unit = {
		val com = do_accept
		logDebug(this + " got connection: " + com + " and provides " + identifier)

		var running = true
		while (running) {
			com.recvOne() match {
				case num: MsgIteratorReqNextN => {
					val q = Queue[T]()
					if (delegate.isInstanceOf[NextIterator[T]] && delegate.hasNext) {
						// No prefetching here. Calling next() multiple times on NextIterator and
						// results in all elements being the same :/)
						q += delegate.next
					} else {
						for (_ <- 1 to num.num if delegate.hasNext) {
							q += delegate.next
						}
					}
					val qe = if (doEncrypt) q.map { n => Encrypt(n) } else q
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

	override def toString() = getClass.getSimpleName + "(identifier=" + identifier + ")"
}
