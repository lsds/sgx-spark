package org.apache.spark.sgx

import scala.reflect.ClassTag

import java.net._
import java.io._
import scala.io._


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

object SgxFail extends SgxReply("fail") {}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
		val socket = new Socket(InetAddress.getByName("localhost"), 9999)
		val oos = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()))
		val ois = new ObjectInputStreamWithCustomClassLoader(new BufferedInputStream(socket.getInputStream()))

		// Send object
		oos.writeObject(SgxMapPartitionsRDDObject(f, partIndex))
		oos.flush()

		// Receive reply
		ois.readObject() match {
			case SgxAck => println("Remote execution of " + f.getClass.getName + " succeeded")
			case SgxFail => println("Remote execution of " + f.getClass.getName + " failed")
		}

		oos.close()
		ois.close()
		socket.close()

		f(partIndex, it)
	}
}