package org.apache.spark.sgx.iterator

import java.util.concurrent.{Executors, Callable, ExecutorCompletionService}

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.Decrypt
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier, providerIsInEnclave: Boolean) extends Iterator[T] with Logging {

	logDebug(this.getClass.getSimpleName + " connecting to: " + id)
	private val com = id.connect()
	private var closed = false

	private val objects = Queue[T]()

	val completor = new ExecutorCompletionService[Queue[T]](Executors.newCachedThreadPool()) {}

	var filling = false

	override def hasNext: Boolean = {
		fill()
		if (objects.length > 0) true
		else if (closed) false
		else {
			objects.length > 0
		}
	}

	override def next: T = {
		fill()
		// This assumes that a next object exist. The caller needs to ensure
		// this by preceeding this call with a call to hasNext()
		objects.dequeue
	}

	def fill(): Unit = synchronized {
		if (closed) return

		if (objects.length > 0) {
			if (!filling) {
				if (objects.length < SgxSettings.PREFETCH / 2) {
					completor.submit(new Filler(com, SgxSettings.PREFETCH - objects.length))
					filling = true
				}
			} else {
				val future = completor.poll()
				if (future != null) {
					val list = future.get
					if (list.length == 0) close()
					else objects ++= list
					filling = false
				}
			}
		}
		else {
			if (!filling) {
				completor.submit(new Filler(com, SgxSettings.PREFETCH - objects.length))
				filling = true
			}
			val list = completor.take().get
			if (list.length == 0) close()
			else objects ++= list
			filling = false
		}
	}

	def close() = {
		closed = true
		com.sendOne(MsgIteratorReqClose)
		com.close()
	}

	override def toString() = getClass.getSimpleName + "(id=" + id + ")"

	class Filler[T](com: SgxCommunicator, num: Int) extends Callable[Queue[T]] {
		def call(): Queue[T] = {
			val list = com.sendRecv[Queue[Any]](new MsgIteratorReqNextN(num))
			list.map {
				n => val m = if (providerIsInEnclave) {
					n match {
						case x: scala.Tuple2[String,Any] => new scala.Tuple2[Any,Any](Decrypt(x._1, SgxSettings.ENCRYPTION_KEY), x._2)
						case x: Any => x
					}
				} else n
				n.asInstanceOf[T]
			}
		}

		override def toString() = getClass.getSimpleName + "(com=" + com + ", num=" + num + ")"
	}
}
