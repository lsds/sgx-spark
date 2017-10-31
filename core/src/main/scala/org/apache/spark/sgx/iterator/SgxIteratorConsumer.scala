package org.apache.spark.sgx.iterator

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

	override def hasNext: Boolean = {
		if (objects.length > 0) true
		else if (closed) false
		else {
			get_next(SgxSettings.PREFETCH)
			objects.length > 0
		}
	}

	override def next: T = {
		// This assumes that a next object exist. The caller needs to ensure
		// this by preceeding this call with a call to hasNext()
//		if (objects.length == 0) get_next(SgxSettings.PREFETCH)
		objects.dequeue
	}

	private def get_next(num: Int): Unit = {
			val list = com.sendRecv[Queue[Any]](new MsgIteratorReqNextN(num))
			if (list.length == 0) close()
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

	def close() = {
		closed = true
		com.sendOne(MsgIteratorReqClose)
		com.close()
	}

	override def toString() = getClass.getSimpleName + "(id=" + id + ")"
}

//class SgxIteratorConsumerRunner[T]