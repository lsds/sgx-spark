package org.apache.spark.sgx

import java.net.ServerSocket

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext
import java.net.ConnectException
import java.net.InetAddress
import java.net.Socket
import java.util.UUID

class SgxMsg(val s: String) extends Serializable

object SgxMsgIteratorReqHasNext extends SgxMsg("iterator.req.hasNext") {}
object SgxMsgIteratorReqNext extends SgxMsg("iterator.req.next") {}
object SgxMsgIteratorReqClose extends SgxMsg("iterator.req.close") {}

case class SgxMsgAccessFakeIterator(fakeId: UUID) extends SgxMsg("iterator.fake.access") {}

class SgxIteratorProviderIdentifier(val host: String, val port: Int) extends Serializable {
	override def toString() = "SgxIteratorProviderIdentifier(host=" + host + ", port=" + port + ")"
}

class SgxIteratorProvider[T](delegate: Iterator[T], host: String) extends InterruptibleIterator[T](null, delegate) with Runnable {
	val myport = 40000 + scala.util.Random.nextInt(10000)
	val identifier = new SgxIteratorProviderIdentifier(host, myport)

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the socket interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override def next(): T = throw new UnsupportedOperationException(s"Access this special Iterator via port $myport.")

	def run = {
		println(s"SgxIteratorProvider now listening on port $myport")
		val sh = new SocketHelper(new ServerSocket(myport).accept())
		println(s"SgxIteratorProvider accepted connection on port $myport")
		var running = true
		while(running) {
			sh.recvOne() match {
				case SgxMsgIteratorReqHasNext =>
					sh.sendOne(super.hasNext)
				case SgxMsgIteratorReqNext =>
					sh.sendOne(super.next)
				case SgxMsgIteratorReqClose =>
					stop(sh)
					running = false
				case x: Any =>
					println(s"SgxIteratorProvider($myport): Unknown input message provided.")
			}
		}
	}

	def stop(sh: SocketHelper) = {
		println(s"Stopping SgxIteratorServer on port $myport")
		sh.close()
	}

	override def toString(): String = {
		this.getClass.getSimpleName + "(host=" + host + ", port=" + myport + ", identifier=" + identifier + ")"
	}
}

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier) extends Iterator[T] {

	println("Connecting to: " + id.host + " "  + id.port)
	private val sh = new SocketHelper(Retry(10)(new Socket(InetAddress.getByName(id.host), id.port)))
	private var closed = false

	override def hasNext: Boolean = {
		if (closed) false
		else {
			val hasNext = sh.sendRecv[Boolean](SgxMsgIteratorReqHasNext)
			if (!hasNext) close()
			hasNext
		}
	}

	override def next: T = {
		if (closed) throw new RuntimeException("Iterator was closed.")
		else sh.sendRecv[T](SgxMsgIteratorReqNext)
	}

	def close() = {
		closed = true
		sh.sendOne(SgxMsgIteratorReqClose)
		sh.close()
	}
}

case class FakeIterator[T](id: UUID) extends Iterator[T] with Serializable {
	override def hasNext: Boolean =  throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")
	override def next: T = throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")

	override def toString = "FakeIterator(" + id + ")"
}
