package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.{Executors, ExecutorService}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import scala.reflect.ClassTag

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.SocketOpenSendRecvClose
import org.apache.spark.sgx.sockets.SocketHelper

import gnu.trove.map.hash.TLongObjectHashMap

class SgxExecuteInside[R] extends Serializable {
  def executeInsideEnclave(): R = {
    SocketOpenSendRecvClose[R](SocketEnv.getIpFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_IP"), SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"), this)
  }
}

case class SgxFirstTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
		extends SgxExecuteInside[Iterator[U]] {

  def apply(): Iterator[U] = Await.result(Future { f(partIndex, new SgxIteratorConsumer[T](id, false, 3)) }, Duration.Inf)
  override def toString = this.getClass.getSimpleName + "(f=" +  f + ", partIndex=" + partIndex + ", id=" + id + ")"
}

case class SgxOtherTask[U,T] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: FakeIterator[T]) extends SgxExecuteInside[Iterator[U]] {

  def apply(realit: Iterator[Any]): Iterator[U] = Await.result(Future { f(partIndex, realit.asInstanceOf[Iterator[T]]) }, Duration.Inf)
  override def toString = this.getClass.getSimpleName + "(f=" +  f + ", partIndex=" + partIndex + ", it=" + it + ")"
}

case class SgxFct2[A,B,Z](
    fct: (A, B) => Z,
    a: A,
    b: B) extends SgxExecuteInside[Z] {

  def apply(): Z = Await.result(Future { fct(a, b) }, Duration.Inf)
  override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

class IdentifierManager[T,F](c: Long => F) {
	private val identifiers = new TLongObjectHashMap[T]()

	def create(obj: T): F = this.synchronized {
		val uuid = scala.util.Random.nextLong
		identifiers.put(uuid, obj)
		c(uuid)
	}

	def get(id: Long): T = this.synchronized {
		identifiers.get(id)
	}

	def remove(id: Long): T = this.synchronized {
		identifiers.remove(id)
	}
}

class SgxMainRunner(s: Socket, fakeIterators: IdentifierManager[Iterator[Any],FakeIterator[Any]]) extends Runnable {
	def run() = {
		val sh = new SocketHelper(s)

		sh.sendOne(sh.recvOne() match {
			case x: SgxFct2[_,_,_] => x.apply()
			case x: SgxFirstTask[_,_] => fakeIterators.create(x.apply())
			case x: SgxOtherTask[_,_] => fakeIterators.create(x.apply(fakeIterators.remove(x.it.id)))

			case x: MsgAccessFakeIterator =>
				val iter = new SgxIteratorProvider[Any](fakeIterators.get(x.fakeId), true, 3)
				new Thread(iter).start
				iter.identifier
		})

		sh.close()
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {
		val fakeIterators = new IdentifierManager[Iterator[Any],FakeIterator[Any]](FakeIterator(_))
		val server = new ServerSocket(SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"))
		val pool: ExecutorService = Executors.newFixedThreadPool(100)

		println("Main: Waiting for connections on port " + server.getLocalPort)

		try {
			while (true) {
				pool.execute(new SgxMainRunner(server.accept(), fakeIterators))
			}
		}
		finally {
			server.close()
			pool.shutdown()
		}
	}
}
