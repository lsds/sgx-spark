package org.apache.spark.sgx

import java.net.InetAddress
import java.net.Socket
import scala.reflect.ClassTag


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
	def computeSpec(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: SgxIteratorServerBinding[T]): Iterator[U] = {
		val sh = new SocketHelper(new Socket(InetAddress.getByName(it.getTheirHost()), it.getTheirPort()))

		sh.sendOne(SgxMapPartitionsRDDObject(f, partIndex))

		val results = sh.recvMany()



//		// Send SgxMapPartitionsRDDObject object and data objects
//		sh.sendOne(SgxMapPartitionsRDDObject(f, partIndex))
//		sh.sendMany(it)
//
//		// Receive the results
//		val results = sh.recvMany()
//
		sh.close()

		results.asInstanceOf[Iterator[U]]
//		new Iterator[U] {
//			override def next(): U = 0.asInstanceOf[U]
//			override def hasNext(): Boolean = false
//		}
	}

	def computeGen(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
//		val sh = new SocketHelper(new Socket(InetAddress.getByName("localhost"), 9999))
		printf("general compute().")

//		// Send SgxMapPartitionsRDDObject object and data objects
//		sh.sendOne(SgxMapPartitionsRDDObject(f, partIndex))
//		sh.sendMany(it)
//
//		// Receive the results
//		val results = sh.recvMany()
//
//		sh.close()
//
//		results.asInstanceOf[Iterator[U]]
//		new Iterator[U] {
//			override def next(): U = 0.asInstanceOf[U]
//			override def hasNext(): Boolean = false
//		}
		f(partIndex, it)
	}
}