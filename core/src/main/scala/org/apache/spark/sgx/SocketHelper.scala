package org.apache.spark.sgx

import java.io.ObjectOutputStream
import java.net.Socket

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

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

	def sendMany(it: Iterator[Any]): Unit = {
		it.foreach {
			x => sendOne(x)
		}
		sendOne(SgxDone)
	}

	def recvMany(): Iterator[Any] = {
		var list = new ListBuffer[Any]()
		breakable {
			while(true) {
				recvOne() match {
					case SgxDone => break
					case x: Any => list += x
				}
			}
		}
		list.iterator
	}

	def close() = {
		oos.close()
		ois.close()
		socket.close()
	}
}
