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

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import java.util.UUID

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.SocketOpenSendRecvClose
import org.apache.spark.sgx.sockets.SocketHelper


abstract class SgxTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int)
		extends Serializable {
  
  def executeInsideEnclave(): Iterator[U] = {
    SocketOpenSendRecvClose[Iterator[U]](SocketEnv.getIpFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_IP"), SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"), this)
  }
}

case class SgxFirstTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
		extends SgxTask[U,T](f, partIndex) {
  
  def apply(): Iterator[U] = {
    val future = Future {
			f(partIndex, new SgxIteratorConsumer[T](id, false, 3))
		}

		Await.result(future, Duration.Inf)
  }
}

case class SgxOtherTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	it: FakeIterator[T])
		extends SgxTask[U,T](f, partIndex) {
  
  def apply(realit: Iterator[Any]): Iterator[U] = {
    val future = Future {
			f(partIndex, realit.asInstanceOf[Iterator[T]])
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
	def run() = {
		val sh = new SocketHelper(s)

		sh.sendOne(sh.recvOne() match {
			case x: SgxFirstTask[_,_] => 
			  fakeIterators.create(x.apply())

			case x: SgxOtherTask[_,_] => 
			  fakeIterators.create(x.apply(fakeIterators.remove(x.it.id)))

			case x: MsgAccessFakeIterator =>
				val iter = new SgxIteratorProvider[Any](fakeIterators.get(x.fakeId), true, 3)
				new Thread(iter).start()
				iter.identifier
		})
		sh.close()
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {
		val fakeIterators = new IdentifierManager[Iterator[Any],FakeIterator[Any]](FakeIterator(_))
		val server = new ServerSocket(SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"))
	  println("Main: Waiting for connections on port " + server.getLocalPort)
		
		while (true) {
			new Thread(new SgxMainRunner(server.accept(), fakeIterators)).start()
		}
		server.close()
	}
}
