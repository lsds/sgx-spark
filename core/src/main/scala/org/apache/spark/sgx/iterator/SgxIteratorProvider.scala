package org.apache.spark.sgx.iterator

import java.util.concurrent.Callable

import scala.collection.mutable.Queue

import org.apache.spark.InterruptibleIterator
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings

abstract class SgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean) extends InterruptibleIterator[T](null, null) with Callable[Unit] with Logging {
	protected def do_accept: SgxCommunicator

	val identifier: SgxIteratorProviderIdentifier

	override final def hasNext: Boolean = delegate.hasNext

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the message interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override final def next(): T = throw new UnsupportedOperationException(s"Access this special Iterator via messages.")

	def call(): Unit = {
		logDebug(this + " now waiting for connections")
		val com = do_accept
		logDebug(this + " got connection: " + com)

		var running = true
		while (running) {
			com.recvOne() match {
				case num: MsgIteratorReqNextN => {
					val q = Queue[Any]()
					if (delegate.isInstanceOf[NextIterator[T]]) {
						logDebug(this + "Providing ONE (" + delegate.getClass.getSimpleName + ")")
						// No Prefetching here. Calling next() multiple times on NextIterator and
						// results in all elements being the same :/)
						if (delegate.hasNext) q += delegate.next
						if (delegate.hasNext) q += delegate.next
						if (delegate.hasNext) q += delegate.next
						if (delegate.hasNext) q += delegate.next
					} else {
						for (_ <- 1 to num.num) {
							if (delegate.hasNext) {
								val n = delegate.next
								q.enqueue(if (inEnclave) {
									n match {
										case x: scala.Tuple2[String, _] => new scala.Tuple2[String, Any](Encrypt(x._1, SgxSettings.ENCRYPTION_KEY), x._2)
										case x: Any => x
									}
								} else n)
							}
						}
						logDebug(this + "Providing " + q.size)
					}
					logDebug("Sending: " + q)
					com.sendOne(q)
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
