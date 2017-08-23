package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket
import java.net.Socket

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

import org.apache.spark.sgx.SgxEnvVar

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
		println("  SgxFirstTaskApply: Starting(" + obj.f.getClass.getName + ")")
		val future = Future {
			println("  SgxFirstTaskApply: Start preparing Future")
			val it = new SgxIteratorConsumer[Any](obj.id)
			val res = obj.f(obj.partIndex, it)
			println("  SgxFirstTaskApply: End preparing Future")
			res
		}

		println("  SgxFirstTaskApply: Await result of Future")
		val res = Await.result(future, Duration.Inf)
		println("  SgxFirstTaskApply: Result of Future received")
		res
	}
}

class SgxOtherTaskApply(obj: SgxOtherTask[Any,Any], realit: Iterator[Any]) {
	def doit(): Iterator[Any] = {
		println("Starting SgxOtherTaskApply(" + obj.f.getClass.getName + ")")
		val future = Future {
			obj.f(obj.partIndex, realit)
		}

		Await.result(future, Duration.Inf)
	}
}

class IdentifierManager[T,F](c: UUID => F) {
	private var identifiers = new TrieMap[UUID,T]()

	def create(obj: T): F = this.synchronized {
		val uuid = UUID.randomUUID()
		identifiers = identifiers ++ List(uuid -> obj)
		c(uuid)
	}

	def get(id: UUID): T = this.synchronized {
		identifiers.apply(id)
	}

	def remove(id: UUID): T = this.synchronized {
		identifiers.remove(id).get
	}
}

class SgxMainRunner(s: Socket, fakeIterators: IdentifierManager[Iterator[Any],FakeIterator[Any]]) extends Runnable {
	val name = s"  Runner(" + s + ")"
	println(name + ": initialized.")

	def run() = {
		println(name + ": starting")
		val sh = new SocketHelper(s)

		val in = sh.recvOne()
		println(name + ": Reading: " + in)

		val reply = in match {
			case x: SgxFirstTask[_,_] =>
				val it = new SgxFirstTaskApply(x.asInstanceOf[SgxFirstTask[Any,Any]]).doit()
				fakeIterators.create(it)

			case x: SgxOtherTask[_,_] =>
				val iter = fakeIterators.remove(x.it.id)
				val it = new SgxOtherTaskApply(x.asInstanceOf[SgxOtherTask[Any,Any]], iter).doit()
				fakeIterators.create(it)

			case x: SgxMsgAccessFakeIterator =>
				println(name + ": Accessing iterator")
				val iter = new SgxIteratorProvider[Any](fakeIterators.get(x.fakeId), SgxEnvVar.getIpFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_IP"))
				println(name + ": Retrieved: " + iter + "("+iter.getClass.getName+")")
				new Thread(iter).start()
				iter.identifier
		}
		println(name + ": Returning " + reply + " (" + reply.getClass.getName + ")")
		sh.sendOne(reply)

		sh.close()
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {
		val server = new ServerSocket(SgxEnvVar.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"))
		val fakeIterators = new IdentifierManager[Iterator[Any],FakeIterator[Any]](FakeIterator(_))
		while (true) {
			println("Main: Waiting for connections on port " + server.getLocalPort)
			new Thread(new SgxMainRunner(server.accept(), fakeIterators)).start()
		}
		server.close()
	}
}
