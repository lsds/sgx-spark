package org.apache.spark.sgx.iterator

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.Decrypt
import org.apache.spark.sgx.SgxCommunicationInterface
import org.apache.spark.sgx.SgxSettings

trait SgxIteratorProviderIdentifier extends Serializable {
	def connect(): SgxCommunicationInterface
}

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier, providerIsInEnclave: Boolean) extends Iterator[T] with Logging {

	logDebug(this.getClass.getSimpleName + " connecting to: " + id)
	private val com = id.connect()
	private var closed = false

	private val objects = Queue[T]()

	override def hasNext: Boolean = {
		if (objects.length > 0) true
		else if (closed) false
		else {
			val hasNext = com.sendRecv[Boolean](MsgIteratorReqHasNext)
			if (!hasNext) close()
			hasNext
		}
	}

	override def next: T = {
		if (closed) throw new NoSuchElementException("Iterator was closed.")
		else if (objects.length == 0) {
			val list = com.sendRecv[Queue[Any]](MsgIteratorReqNext)
			objects ++= list.map {
					n => val m = if (providerIsInEnclave) {
						n match {
							case x: scala.Tuple2[String,Any] => new scala.Tuple2[Any,Any](Decrypt(x._1, SgxSettings.ENCRYPTION_KEY), x._2)
							case x: Any => x
						}
					} else n
					n.asInstanceOf[T]
				}
		}
		objects.dequeue
	}

	def close() = {
		closed = true
		com.sendOne(MsgIteratorReqClose)
		com.close()
	}
}