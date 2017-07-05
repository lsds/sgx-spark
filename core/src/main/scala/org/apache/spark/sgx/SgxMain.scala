package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import scala.reflect.ClassTag
import java.util.UUID
import jersey.repackaged.com.google.common.collect.Synchronized

class ObjectInputStreamWithCustomClassLoader(inputStream: InputStream) extends ObjectInputStream(inputStream) {
	override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
		try {
			Class.forName(desc.getName, false, getClass.getClassLoader)
		} catch {
			case ex: ClassNotFoundException => super.resolveClass(desc)
		}
	}
}
class SgxSuperTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int)
		extends Serializable {
}

case class SgxFirstTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
		extends SgxSuperTask[U,T](f, partIndex) {
}

case class SgxOtherTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: FakeIterator[T])
		extends SgxSuperTask[U,T](f, partIndex) {
}

class SgxFirstTaskApply(obj: SgxFirstTask[Any,Any]) {
	def doit(): Iterator[Any] = {
		val future = Future {
			val it = new SgxIteratorConsumer[Any](obj.id)
			obj.f(obj.partIndex, it)
		}

		Await.result(future, Duration.Inf)
	}
}

class SgxOtherTaskApply(obj: SgxOtherTask[Any,Any], realit: Iterator[Any]) {
	def doit(): Iterator[Any] = {
		val future = Future {
			obj.f(obj.partIndex, realit)
		}

		Await.result(future, Duration.Inf)
	}
}

class IdentifierManager[T,F](c: UUID => F) {
	private var identifiers = new TrieMap[UUID,T]()

	def create(obj: T): F = {
		val uuid = UUID.randomUUID()
		identifiers = identifiers ++ List(uuid -> obj)
		c(uuid)
	}

	def get(id: UUID): T = identifiers.apply(id)
	def remove(id: UUID): T = identifiers.remove(id).get
}

object SgxMain {
	def main(args: Array[String]): Unit = {

		val server = new ServerSocket(9999)
		val fakeIterators = new IdentifierManager[Iterator[Any],FakeIterator[Any]](FakeIterator(_))

		while (true) {
			val sh = new SocketHelper(server.accept())

			sh.recvOne() match {
				case x: SgxFirstTask[_,_] =>
					val it = new SgxFirstTaskApply(x.asInstanceOf[SgxFirstTask[Any,Any]]).doit()
					val fakeit = fakeIterators.create(it)
					sh.sendOne(fakeit)
					sh.close()

				case x: SgxOtherTask[_,_] =>
					val iter = fakeIterators.remove(x.it.id)
					val it = new SgxOtherTaskApply(x.asInstanceOf[SgxOtherTask[Any,Any]], iter).doit()
					val fakeit = fakeIterators.create(it)
					sh.sendOne(fakeit)
					sh.close()

				case x: SgxMsgAccessFakeIterator =>
					val iter = new SgxIteratorProvider[Any](fakeIterators.get(x.fakeId))
					new Thread(iter).start()
					sh.sendOne(iter.identifier)
					sh.close()
			}
		}

		server.close()
	}
}
