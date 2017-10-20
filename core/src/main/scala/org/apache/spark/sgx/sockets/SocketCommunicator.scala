package org.apache.spark.sgx.sockets

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings

class SocketCommunicator(socket: Socket) extends SgxCommunicator {
	private	val oos = new ObjectOutputStream(socket.getOutputStream())
	private	val ois = new ObjectInputStream(socket.getInputStream())

	def close() = {
		oos.close()
		ois.close()
		socket.close()
	}

	def read(): AnyRef = {
		ois.readObject()
	}

	def write(o: Any) = {
		oos.reset()
		oos.writeObject(o)
		oos.flush()
	}

	override def toString() = getClass.getSimpleName + "(local=" + socket.getLocalAddress + ":" + socket.getLocalPort + ", remote=" + socket.getRemoteSocketAddress + ":" + socket.getPort + ")"
}

/**
 * Inspired by
 * https://stackoverflow.com/questions/7930814/whats-the-scala-way-to-implement-a-retry-able-call-like-this-one
 */
object Retry {
	def apply[T](n: Int)(fn: => T): T = {
		val r = try { Some(fn) } catch { case e: Exception if n > 1 => None }
		r match {
			case Some(x) => x
			case None => apply(n - 1)(fn)
		}
	}
}
