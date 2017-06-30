package org.apache.spark.sgx

import java.net.Socket

import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import scala.collection.mutable.ListBuffer
import org.apache.spark.sgx.SgxMsg

private object SgxDone extends SgxMsg("done") {}

class SocketHelper(socket: Socket) {

	private	val oos = new ObjectOutputStream(socket.getOutputStream())
	private	val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

	def sendOne(obj: Any) = {
//		println("  Sending: " + obj + " (" + obj.getClass.getName + ")")
		oos.reset()
		oos.writeObject(obj)
		oos.flush()
	}

	def recvOne(): Any = {
		val o = ois.readObject()
//		println("  Receiving: " + o + " (" + o.getClass.getName + ")")
		o
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
//		println("  Received number of objects: " + list.size)
		list.iterator
	}

	def close() = {
		oos.close()
		ois.close()
		socket.close()
	}
}
