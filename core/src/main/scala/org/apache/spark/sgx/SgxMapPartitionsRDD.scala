package org.apache.spark.sgx

import scala.reflect.ClassTag

import java.net.InetAddress
import java.net.Socket

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import scala.collection.mutable.ListBuffer


case class SgxMapPartitionsRDDObject[U: ClassTag, T: ClassTag](
	f: (Int, Iterator[T]) => Iterator[U],
		partIndex: Int)
	extends Serializable {
	override def toString = s"SgxMapPartitionsRDDObject($f, $partIndex)"
}

object SocketHelper {
	def send(oos: ObjectOutputStream, obj: Any): Unit = {
		oos.reset()
		oos.writeObject(obj)
		oos.flush()
	}

	def read(ois: ObjectInputStream): Any = {
		ois.readObject()
	}

//	def readMany(ois: ObjectInputStream, end: SgxMsg = SgxDone): ListBuffer[Any] = {
//
//	}
}

class SgxMsg(s : String)
	extends Serializable {
	override def toString = s"SgxReply($s)"
}

object SgxAck extends SgxMsg("ack") {}
object SgxDone extends SgxMsg("done") {}
object SgxFail extends SgxMsg("fail") {}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {

	def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
		val socket = new Socket(InetAddress.getByName("localhost"), 9999)
		val oos = new ObjectOutputStream(socket.getOutputStream())
		val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

		// Send object
		println("Sending: " + SgxMapPartitionsRDDObject(f, partIndex))
		SocketHelper.send(oos, SgxMapPartitionsRDDObject(f, partIndex))

		it.foreach {
			x => println("Sending: " + x + " (" + x.getClass.getName + ")")
			SocketHelper.send(oos, x)
		}
		SocketHelper.send(oos, SgxDone)

		var list = new ListBuffer[Any]()
		breakable {
			while(true) {
				SocketHelper.read(ois) match {
					case SgxDone => break
					case x: Any => {
						println("  Receiving: " + x + " (" + x.getClass.getName + ")")
						list += x
					}
				}
			}
		}
		println("  Received number of objects: " + list.size)

		oos.close()
		ois.close()
		socket.close()

		list.iterator.asInstanceOf[Iterator[U]]
	}
}