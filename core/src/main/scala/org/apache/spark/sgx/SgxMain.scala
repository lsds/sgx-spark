package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.{Executors, CompletionService, Callable, ExecutorCompletionService, LinkedBlockingDeque}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import scala.reflect.ClassTag

import org.apache.spark.sgx.sockets.Retry

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.SocketOpenSendRecvClose
import org.apache.spark.sgx.sockets.SocketHelper

import org.apache.spark.internal.Logging

import gnu.trove.map.hash.TLongObjectHashMap

import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.Retry

import java.lang.reflect.Field;
import sun.misc.Unsafe;

class SgxExecuteInside[R] extends Serializable {
  def executeInsideEnclave(): R = {
    ClientHandle.sendRecv[R](this)
  }
}

case class SgxFirstTask[U: ClassTag, T: ClassTag] (
	f: (Int, Iterator[T]) => Iterator[U],
	partIndex: Int,
	id: SgxIteratorProviderIdentifier)
		extends SgxExecuteInside[Iterator[U]] {

  def apply(): Iterator[U] = Await.result(Future { f(partIndex, new SgxIteratorConsumer[T](id, false)) }, Duration.Inf)
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

object ClientHandle {
	private val availableHandles = new LinkedBlockingDeque[SocketHelper]()

	0 to SgxSettings.CONNECTIONS foreach { _ =>
		availableHandles.add(new SocketHelper(new Socket(SgxSettings.ENCLAVE_IP, SgxSettings.ENCLAVE_PORT)))
	}

	def sendRecv[O](in: Any) = {
		val h = availableHandles.synchronized {
			availableHandles.take
		}
		val ret = h.sendRecv[O](in)
		availableHandles.synchronized {
			availableHandles.add(h)
		}
		ret
	}
}

class SgxMainRunner(s: Socket, fakeIterators: IdentifierManager[Iterator[Any],FakeIterator[Any]]) extends Callable[Unit] {
	def call(): Unit = {
		val sh = new SocketHelper(s)

		while(true) {
			sh.sendOne(sh.recvOne() match {
				case x: SgxFct2[_,_,_] => x.apply()
				case x: SgxFirstTask[_,_] => fakeIterators.create(x.apply())
				case x: SgxOtherTask[_,_] => fakeIterators.create(x.apply(fakeIterators.remove(x.it.id)))

				case x: MsgAccessFakeIterator =>
					val iter = new SgxIteratorProvider[Any](fakeIterators.get(x.fakeId), true)
					new Thread(iter).start
					iter.identifier
			})
		}

		sh.close()
	}

	override def toString() = this.getClass.getSimpleName + "(port="+s.getLocalPort+")"
}

object Completor extends ExecutorCompletionService[Unit](Executors.newFixedThreadPool(32)) {}

class Waiter() extends Callable[Unit] {
       def call(): Unit = while (true) Completor.take
}

object SgxMain extends Logging {
	def shmem(): Unit = {

		val writer = new RingBuff(SgxSettings.SHMEM_ENC_TO_OUT)
		val reader = new RingBuff(SgxSettings.SHMEM_OUT_TO_ENC)

//		System.out.println("written: " + rb.write(new java.lang.Long(1)));
//		System.out.println("written: " + rb.write(new java.lang.Integer(3)));
		System.out.println("written: " + writer.write(new java.io.IOException("Foobar")));
//		System.out.println("   read: " + rb.read());
//		System.out.println("   read: " + rb.read());
//		System.out.println("   read: " + rb.read());
	}

	def main(args: Array[String]): Unit = {
		shmem()
		val fakeIterators = new IdentifierManager[Iterator[Any],FakeIterator[Any]](FakeIterator(_))
		val server = new ServerSocket(SgxSettings.ENCLAVE_PORT)
		val completion = new ExecutorCompletionService[Unit](Executors.newFixedThreadPool(100))

		logDebug("Main: Waiting for connections on port " + server.getLocalPort)

		Completor.submit(new Waiter())
		try {
			while (true) {
				completion.submit(new SgxMainRunner(server.accept(), fakeIterators))
			}
		}
		finally {
			server.close()
		}
	}
}
