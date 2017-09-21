package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.{Executors, CompletionService, Callable, ExecutorCompletionService}

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

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.Retry

private object MsgNewGenericService extends Serializable {}
private object MsgCloseGenericService extends Serializable {}

class SgxExecuteInside[R] extends Serializable {
  def executeInsideEnclave(): R = {
	  print(this + "executeInsideEnclave() ...")
	  val x = GenericServiceClientHandle.sendRecv[R](this)
//    SocketOpenSendRecvClose[R](SocketEnv.getIpFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_IP"), SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"), this)
	  println("done")
	  x
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

class GenericService(fakeIterators: IdentifierManager[Iterator[Any],FakeIterator[Any]]) extends Callable[Unit] {
	val port = SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT") - 1

	def call(): Unit = {
		val sh = new SocketHelper(new ServerSocket(port).accept())
		var running = true
		while(running) {
			println(this + " waiting for packets")
			sh.recvOne() match {
				case x: SgxFct2[_,_,_] => println(this + "  incoming: " + x); sh.sendOne(x.apply())
				case x: SgxFirstTask[_,_] => println(this + "  incoming: " + x); sh.sendOne(fakeIterators.create(x.apply()))
				case x: SgxOtherTask[_,_] => println(this + "  incoming: " + x); sh.sendOne(fakeIterators.create(x.apply(fakeIterators.remove(x.it.id))))

				case MsgCloseGenericService =>
					println(this + "  incoming: MsgCloseGenericService");
					stop(sh)
					running = false

				case _ =>
					println(this + ": Unknown input message provided.")
			}
		}
	}

	def stop(sh: SocketHelper) = {
		sh.close()
	}

	override def toString() = this.getClass.getSimpleName + "(port="+port+")"
}

object GenericServiceClientHandle {
	private val sh = new SocketHelper(Retry(10)(new Socket(SocketEnv.getIpFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_IP"), SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT") - 1)))
	println(this + " established connection")

	def sendRecv[R](in: Any): R = this.synchronized {
		println(this + " sending: " + in)
		sh.sendRecv(in)
	}

	override def toString() = this.getClass.getSimpleName  + "(remoteHost="+SocketEnv.getIpFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_IP")+", remotePort="+(SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT") - 1) +")"
}


class SgxMainRunner(s: Socket, fakeIterators: IdentifierManager[Iterator[Any],FakeIterator[Any]], completion: ExecutorCompletionService[Unit]) extends Callable[Unit] {
	def call(): Unit = {
		val sh = new SocketHelper(s)

		sh.sendOne(sh.recvOne() match {
			case x: SgxFct2[_,_,_] => x.apply()
			case x: SgxFirstTask[_,_] => fakeIterators.create(x.apply())
			case x: SgxOtherTask[_,_] => fakeIterators.create(x.apply(fakeIterators.remove(x.it.id)))

			case x: MsgAccessFakeIterator =>
				val iter = new SgxIteratorProvider[Any](fakeIterators.get(x.fakeId), true, 3)
				completion.submit(iter)
				iter.identifier
		})

		sh.close()
	}
}

class Waiter(compl: ExecutorCompletionService[Unit]) extends Callable[Unit] {
	def call(): Unit = {
		while (true) compl.take
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {
		val fakeIterators = new IdentifierManager[Iterator[Any],FakeIterator[Any]](FakeIterator(_))
		val server = new ServerSocket(SocketEnv.getPortFromEnvVarOrAbort("SPARK_SGX_ENCLAVE_PORT"))
		val completion = new ExecutorCompletionService[Unit](Executors.newFixedThreadPool(100))

		println("Main: Waiting for connections on port " + server.getLocalPort)

		completion.submit(new Waiter(completion))
		completion.submit(new GenericService(fakeIterators))

		try {
			while (true) {
//				completion.submit(new SgxMainRunner(server.accept(), fakeIterators))
				new SgxMainRunner(server.accept(), fakeIterators, completion).call()
			}
		}
		finally {
			server.close()
		}
	}
}
