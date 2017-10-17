package org.apache.spark.sgx

import org.apache.spark.InterruptibleIterator
import org.apache.spark.util.NextIterator
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.SocketOpenSendRecvClose
import org.apache.spark.sgx.sockets.SocketHelper
import org.apache.spark.sgx.sockets.SgxSocketIteratorProviderIdentifier
import org.apache.spark.sgx.iterator._
import org.apache.spark.sgx.sockets.Retry

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging

class SgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean) extends InterruptibleIterator[T](null, null) with Runnable with Logging {
	val host = if (inEnclave) SgxSettings.ENCLAVE_IP else SgxSettings.HOST_IP
	val port = 40000 + scala.util.Random.nextInt(10000)
	val identifier = new SgxSocketIteratorProviderIdentifier(host, port)

	override def hasNext: Boolean = delegate.hasNext

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the socket interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override def next(): T = throw new UnsupportedOperationException(s"Access this special Iterator via port $port.")

	def run = {
		logDebug(s"SgxIteratorProvider now listening on port $port")
		val sh = new SocketHelper(new ServerSocket(port).accept())
		logDebug(s"SgxIteratorProvider accepted connection on port $port")

		var running = true
		while(running) {
			sh.recvOne() match {
				case MsgIteratorReqHasNext => sh.sendOne(delegate.hasNext)
				case MsgIteratorReqNext => {
					val q = Queue[Any]()
					if (delegate.isInstanceOf[NextIterator[T]]) {
						// No Prefetching here. Calling next() multiple times on NextIterator and
						// results in all elements being the same :/)
						q += delegate.next
					}
					else {
						for (_ <- 1 to SgxSettings.PREFETCH) {
							if (delegate.hasNext) {
								val n = delegate.next
								q.enqueue(if (inEnclave) {
									n match {
										case x: scala.Tuple2[String,_] => new scala.Tuple2[String,Any](Encrypt(x._1, SgxSettings.ENCRYPTION_KEY), x._2)
										case x: Any => x
									}
								} else n)
							}
						}
					}
					sh.sendOne(q)
				}
				case MsgIteratorReqClose =>
					stop(sh)
					running = false

				case x: Any => logDebug(s"SgxIteratorProvider($port): Unknown input message provided.")
			}
		}
	}

	def stop(sh: SocketHelper) = {
		logDebug(s"Stopping SgxIteratorServer on port $port")
		sh.close()
	}

	override def toString() = this.getClass.getSimpleName + "(host=" + host + ", port=" + port + ", identifier=" + identifier + ")"
}
