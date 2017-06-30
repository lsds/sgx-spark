package org.apache.spark.sgx

import java.net.InetAddress
import java.net.Socket

import scala.reflect.ClassTag

class SgxSuperTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int)
		extends Serializable {
	override def toString = s"SgxFirstTask($f, $partIndex)"
}

case class SgxFirstTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorServerIdentifier)
		extends SgxSuperTask[U,T](f, partIndex) {
	override def toString = s"SgxFirstTask($f, $partIndex, $id)"
}

case class SgxOtherTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: Iterator[T])
		extends SgxSuperTask[U,T](f, partIndex) {
}

class SgxMsg(s : String)
	extends Serializable {
	override def toString = s"SgxReply($s)"
}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(t: SgxSuperTask[U,T]): Iterator[U] = {

		/*
		 * TODO
		 * Move the code into a function within the classes. Then just call this function.
		 * Process in parallel
		 * Handle the last task that writes to file (this also is a split iterator)
		 * Handle all tasks between the first and the last. The iterator can stay within the enclave, but the outside expects a handle
		 */
		t match {
			case x: SgxFirstTask[Any,Any] =>
				val sh = new SocketHelper(new Socket(InetAddress.getByName("localhost"), 9999))
				sh.sendOne(x)
				val results = sh.recvMany()
				sh.close()
				results.asInstanceOf[Iterator[U]]
			case x: SgxOtherTask[Any,Any] => x.f(x.partIndex, x.it).asInstanceOf[Iterator[U]]
		}
	}
}