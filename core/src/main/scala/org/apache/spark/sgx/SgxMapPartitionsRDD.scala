package org.apache.spark.sgx

import scala.reflect.ClassTag

import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket

case class SgxMapPartitionsRDDObject[U: ClassTag, T: ClassTag](
	f: (Int, Iterator[T]) => Iterator[U],
		partIndex: Int)
	extends Serializable {
	override def toString = s"SgxMapPartitionsRDDObject($f, $partIndex)"
}

class SgxMsg(s : String)
	extends Serializable {
	override def toString = s"SgxReply($s)"
}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
		val sh = new SocketHelper(new Socket(InetAddress.getByName("localhost"), 9999))

		// Send SgxMapPartitionsRDDObject object and data objects
		sh.sendOne(SgxMapPartitionsRDDObject(f, partIndex))
		sh.sendMany(it)

		// Receive the results
		val results = sh.recvMany()

		sh.close()

		results.asInstanceOf[Iterator[U]]
	}
}