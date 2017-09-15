package org.apache.spark.sgx

import org.apache.spark.InterruptibleIterator
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID
import org.apache.spark.sgx.sockets.SocketEnv
import org.apache.spark.sgx.sockets.SocketOpenSendRecvClose
import org.apache.spark.sgx.sockets.SocketHelper
import org.apache.spark.sgx.sockets.Retry

private object MsgIteratorReqHasNext extends Serializable {}
private object MsgIteratorReqNext extends Serializable {}
private object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator(fakeId: Long) extends Serializable {}

class SgxIteratorProviderIdentifier(val host: String, val port: Int) extends Serializable {
	override def toString() = this.getClass.getSimpleName + "(host=" + host + ", port=" + port + ")"
}

class SgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean, key: Long = 0) extends InterruptibleIterator[T](null, delegate) with Runnable {
	val host = SocketEnv.getIpFromEnvVarOrAbort(if (inEnclave) "SPARK_SGX_ENCLAVE_IP" else "SPARK_SGX_HOST_IP")
	val myport = 40000 + scala.util.Random.nextInt(10000)
	val identifier = new SgxIteratorProviderIdentifier(host, myport)

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the socket interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override def next(): T = throw new UnsupportedOperationException(s"Access this special Iterator via port $myport.")

	def run = {
		println(s"SgxIteratorProvider now listening on port $myport")
		val sh = new SocketHelper(new ServerSocket(myport).accept())
		println(s"SgxIteratorProvider accepted connection on port $myport")
		var running = true
		while(running) {
			sh.recvOne() match {
				case MsgIteratorReqHasNext => sh.sendOne(super.hasNext)
				case MsgIteratorReqNext => {
				  val n = super.next
				  val m = if (inEnclave) {
				    n match {
				      case x: scala.Tuple2[_,_] => new scala.Tuple2[String,Any](Encryption.encrypt(x._1, key), x._2)
				      case x: Any => x
				    }
				  } else n
				  sh.sendOne(m)
				}
				case MsgIteratorReqClose =>
					stop(sh)
					running = false
				case x: Any => println(s"SgxIteratorProvider($myport): Unknown input message provided.")
			}
		}
	}

	def stop(sh: SocketHelper) = {
		println(s"Stopping SgxIteratorServer on port $myport")
		sh.close()
	}

	override def toString() = this.getClass.getSimpleName + "(host=" + host + ", port=" + myport + ", identifier=" + identifier + ")"
}

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier, providerIsInEnclave: Boolean, key: Long = 0) extends Iterator[T] {

	println(this.getClass.getSimpleName + " connecting to: " + id.host + " "  + id.port)
	private val sh = new SocketHelper(Retry(10)(new Socket(id.host, id.port)))
	private var closed = false

	override def hasNext: Boolean = {
		if (closed) false
		else {
			val hasNext = sh.sendRecv[Boolean](MsgIteratorReqHasNext)
			if (!hasNext) close()
			hasNext
		}
	}

	override def next: T = {
		if (closed) throw new NoSuchElementException("Iterator was closed.")
		else {
		  val n = sh.sendRecv[Any](MsgIteratorReqNext)
		  val m = if (providerIsInEnclave) {
		    n match {
		      case x: scala.Tuple2[String,Any] => new scala.Tuple2[Any,Any](Encryption.decrypt(x._1, key), x._2)
		      case x: Any => x
		    }
		  } else n
		  m.asInstanceOf[T]
		}
	}

	def close() = {
		closed = true
		sh.sendOne(MsgIteratorReqClose)
		sh.close()
	}
}

case class FakeIterator[T](id: Long) extends Iterator[T] with Serializable {
	override def hasNext: Boolean =  throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")
	override def next: T = throw new RuntimeException("A FakeIterator is just a placeholder and not supposed to be used.")
	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
	
	def access(providerIsInEnclave: Boolean, key: Long = 0): Iterator[T] = {
	  new SgxIteratorConsumer(
          SocketOpenSendRecvClose[SgxIteratorProviderIdentifier](
          SocketEnv.getIpFromEnvVarOrAbort(if (providerIsInEnclave) "SPARK_SGX_ENCLAVE_IP" else "SPARK_SGX_HOST_IP"),
          SocketEnv.getPortFromEnvVarOrAbort(if (providerIsInEnclave) "SPARK_SGX_ENCLAVE_PORT" else "SPARK_SGX_HOST_PORT"),
          MsgAccessFakeIterator(id)), providerIsInEnclave, key)
	}
}
