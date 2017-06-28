package org.apache.spark.sgx

import scala.reflect.ClassTag

import java.net.InetAddress
import java.net.Socket

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
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

class SgxReply(s : String)
	extends Serializable {
	override def toString = s"SgxReply($s)"
}

object SgxAck extends SgxReply("ack") {}
object SgxDone extends SgxReply("done") {}
object SgxFail extends SgxReply("fail") {}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
		println("start of compute")
		val socket = new Socket(InetAddress.getByName("localhost"), 9999)
		val oos = new ObjectOutputStream(socket.getOutputStream())
		val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

//		val (it1,it2) = it.duplicate

		// Send object
		println("Sending: " + SgxMapPartitionsRDDObject(f, partIndex))
		oos.writeUnshared(SgxMapPartitionsRDDObject(f, partIndex))
		oos.flush()

		it.foreach {
			x => println("Sending: " + x + " (" + x.getClass.getName + ")")
			oos.writeUnshared(x)
		}
		oos.writeUnshared(SgxDone)
		oos.flush()

//		println("Sending: " + a + " (" + a.size + ")")
//		a.foreach { x => println("  " + x) }
//		oos.writeUnshared(a)
//		oos.flush()
//
//		// Receive reply
//		ois.readUnshared() match {
//			case SgxAck => println("Remote execution of " + f.getClass.getName + " succeeded")
//			case SgxFail => println("Remote execution of " + f.getClass.getName + " failed")
//		}

		var list = new ListBuffer[Any]()
		breakable {
			while(true) {
				val o = ois.readUnshared() match {
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

//		f(partIndex, it1)
		println("end of compute: returning iterator of size " + list.size)
		list.iterator.asInstanceOf[Iterator[U]]
	}
}