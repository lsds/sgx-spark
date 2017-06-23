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

case class SgxReply(
		s : String)
	extends Serializable {
	override def toString = s"SgxReply($s)"
}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
		val obj = SgxMapPartitionsRDDObject(f, partIndex)

		val socket = new Socket(InetAddress.getByName("localhost"), 9999)
		val oos = new ObjectOutputStream(socket.getOutputStream())
		val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

		oos.writeObject(obj)
		println("Snd object: " + obj + " (" + obj.getClass.getName() + ")")

		println(" start resolving")
		val reply = ois.myReadObject()
		println(" end resolving")
		println("Rcv  reply: " + reply.getOrElse("NONE"))

		socket.close()

		f(partIndex, it)
	}
}