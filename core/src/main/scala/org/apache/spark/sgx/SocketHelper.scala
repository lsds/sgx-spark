package org.apache.spark.sgx

import java.io.ObjectOutputStream
import java.net.Socket

import scala.collection.mutable.ListBuffer
import java.net.InetAddress

private object SgxDone extends SgxMsg("done") {}

class SocketHelper(socket: Socket) {

	private	val oos = new ObjectOutputStream(socket.getOutputStream())
	private	val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

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
 * All in one go:
 * (1) Open connection
 * (2) Send object
 * (3) Get answer
 * (4) Close connection
 */
object SocketAction {
	def apply[I,O](host: String, port: Int, in: I): O = {
		val sh = new SocketHelper(new Socket(InetAddress.getByName(host), port))
		sh.sendOne(in)
		val res = sh.recvOne()
		sh.close()
		res.asInstanceOf[O]
	}
}
