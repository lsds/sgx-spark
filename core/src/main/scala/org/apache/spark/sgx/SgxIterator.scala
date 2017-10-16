package org.apache.spark.sgx

import org.apache.spark.InterruptibleIterator
import org.apache.spark.util.NextIterator
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.SocketOpenSendRecvClose
import org.apache.spark.sgx.sockets.SocketHelper
import org.apache.spark.sgx.sockets.Retry

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging

private object MsgIteratorReqHasNext extends Serializable {}
private object MsgIteratorReqNext extends Serializable {}
private object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator(fakeId: Long) extends Serializable with Logging {}

class SgxIteratorProviderIdentifier(val host: String, val port: Int) extends Serializable {
	override def toString() = this.getClass.getSimpleName + "(host=" + host + ", port=" + port + ")"
}

class SgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean) extends InterruptibleIterator[T](null, null) with Runnable with Logging {
	val host = if (inEnclave) SgxSettings.ENCLAVE_IP else SgxSettings.HOST_IP
	val port = 40000 + scala.util.Random.nextInt(10000)
	val identifier = new SgxIteratorProviderIdentifier(host, port)

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

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier, providerIsInEnclave: Boolean) extends Iterator[T] with Logging {

	logDebug(this.getClass.getSimpleName + " connecting to: " + id.host + " "  + id.port)
	private val sh = new SocketHelper(Retry(SgxSettings.RETRIES)(new Socket(id.host, id.port)))
	private var closed = false

	private val objects = Queue[T]()

	override def hasNext: Boolean = {
		if (objects.length > 0) true
		else if (closed) false
		else {
			val hasNext = sh.sendRecv[Boolean](MsgIteratorReqHasNext)
			if (!hasNext) close()
			hasNext
		}
	}

	override def next: T = {
		if (closed) throw new NoSuchElementException("Iterator was closed.")
		else if (objects.length == 0) {
			val list = sh.sendRecv[Queue[Any]](MsgIteratorReqNext)
			objects ++= list.map {
					n => val m = if (providerIsInEnclave) {
						n match {
							case x: scala.Tuple2[String,Any] => new scala.Tuple2[Any,Any](Decrypt(x._1, SgxSettings.ENCRYPTION_KEY), x._2)
							case x: Any => x
						}
					} else n
					n.asInstanceOf[T]
				}
		}
		objects.dequeue
	}

	def close() = {
		closed = true
		sh.sendOne(MsgIteratorReqClose)
		sh.close()
	}
}

case class FakeIterator[T](id: Long) extends Iterator[T] with Serializable {
	override def hasNext: Boolean =  throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")
	override def next: T = throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")
	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"

	def access(providerIsInEnclave: Boolean, key: Long = 0): Iterator[T] = {
		val iter = if (providerIsInEnclave) ClientHandle.sendRecv[SgxIteratorProviderIdentifier](MsgAccessFakeIterator(id))
			else SocketOpenSendRecvClose[SgxIteratorProviderIdentifier](SgxSettings.HOST_IP, SgxSettings.HOST_PORT, MsgAccessFakeIterator(id))

		new SgxIteratorConsumer(iter, providerIsInEnclave)
	}
}
