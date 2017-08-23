package org.apache.spark.sgx

import java.io.ObjectInputStream

import java.io.ObjectOutputStream
import java.net.Socket

import scala.collection.mutable.ListBuffer
import java.net.InetAddress

private object SgxDone extends SgxMsg("done") {}

class SocketHelper(socket: Socket) {
	val name = "    SocketHelper(" + socket + "): "

	private	val os = socket.getOutputStream()
	private	val oos = new ObjectOutputStream(os)
	private	val is = socket.getInputStream()
	private	val ois = new ObjectInputStreamWithCustomClassLoader(is)

	def sendOne(obj: Any) = {
		oos.reset()
		oos.writeObject(obj)
		oos.flush()
	}

	def recvOne(): Any = {
		ois.readObject()
	}

	def sendRecv[O](in: Any): O = {
		sendOne(in)
		recvOne.asInstanceOf[O]
	}

	def sendMany(it: Iterator[Any]): Unit = {
		it.foreach {
			x => sendOne(x)
		}
		sendOne(SgxDone)
	}

	def recvMany(): Iterator[Any] = {
		var list = new ListBuffer[Any]()
		while(recvOne() match {
			case SgxDone => false
			case x: Any =>
				list += x
				true
		}){}
		list.iterator
	}

	def close() = {
		oos.close()
		ois.close()
		socket.close()
	}
}

/**
 * Inspired by 
 * https://stackoverflow.com/questions/7930814/whats-the-scala-way-to-implement-a-retry-able-call-like-this-one
 */
object Retry {
	def apply[T](n: Int)(fn: => T): T = {
		val r = try { Some(fn) } catch { case e: Exception if n > 1 => None }
		r match {
			case Some(x) => x
			case None => apply(n - 1)(fn)
		}
	}
}

/**
 * All in one go:
 * (1) Open connection
 * (2) Send object
 * (3) Receive answer
 * (4) Close connection
 */
object SocketOpenSendRecvClose {
	def apply[O](host: String, port: Int, in: Any): O = {
		println("Sending to " + host + ":" + port + " => " + in)
		val sh = new SocketHelper(Retry(10)(new Socket(InetAddress.getByName(host), port)))
		sh.sendOne(in)
		println(s"  Sent ok ... Waiting for reply")
		val res = sh.recvOne()
		println(s"  Reply: " + res)
		sh.close()
		res.asInstanceOf[O]
	}
}
