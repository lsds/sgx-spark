package org.apache.spark.sgx.iterator

import java.util.concurrent.{Executors, Callable, ExecutorCompletionService, Future}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.Completor
import org.apache.spark.sgx.Decrypt
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings

class Filler[T](consumer: SgxIteratorConsumer[T]) extends Callable[Unit] with Logging {
	def call(): Unit = {
		val num = SgxSettings.PREFETCH - consumer.objects.size
		if (num > 0) {
			val list = consumer.com.sendRecv[Queue[T]](new MsgIteratorReqNextN(num))
			if (list.size == 0) {
				consumer.close
			}
			else consumer.objects.addAll(list.asJava)
//			else consumer.objects.addAll((list.map {
//				n => val m = if (consumer.providerIsInEnclave) {
//					n match {
//						case x: scala.Tuple2[String,Any] => new scala.Tuple2[Any,Any](Decrypt(x._1, SgxSettings.ENCRYPTION_KEY), x._2)
//						case x: Any => x
//					}
//				} else n
//				n.asInstanceOf[T]
//			}).asJava)
		}
		consumer.Lock.synchronized {
			consumer.fillingFuture = null
		}
	}

	override def toString() = getClass.getSimpleName + "(consumer=" + consumer + ")"
}

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier) extends Iterator[T] with Logging {

	logDebug(this + " connecting to: " + id)

	private var closed = false
	object Lock

	val com = id.connect()
	val objects = new LinkedBlockingQueue[T]()
	var fillingFuture : Future[Unit] = null

	fill()

	override def hasNext: Boolean = {
		fill()
		if (objects.size > 0) true
		else if (closed) false
		else {
			if (fillingFuture != null) fillingFuture.get
			objects.size > 0
		}
	}

	override def next: T = {
		// This assumes that a next object exist. The caller needs to ensure
		// this by preceeding this call with a call to hasNext()
		val n = objects.take
		fill()
		n
	}

	def fill(): Unit = {
		Lock.synchronized {
			if (closed || fillingFuture != null || objects.size > SgxSettings.PREFETCH / 2) return
			else {
				fillingFuture = Completor.submit(new Filler(this))
			}
		}
	}

	def close() = {
		closed = true
		com.sendOne(MsgIteratorReqClose)
		com.close()
	}

	override def toString() = getClass.getSimpleName + "(id=" + id + ")"
}
