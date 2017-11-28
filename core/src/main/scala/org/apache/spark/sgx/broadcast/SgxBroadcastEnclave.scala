package org.apache.spark.sgx.broadcast

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxFactory

import scala.collection.mutable.HashMap

object SgxBroadcastEnclave extends Logging {
	private var com: SgxCommunicator = null

	def init(_com: SgxCommunicator): Unit = {
		com = _com
	}

	def value[T](id: Long): T = {
		logDebug("value("+id+")")
		if (com == null) throw new IllegalStateException(this + " has not been initialized")
		val x = com.sendRecv[T](new MsgBroadcastValue(id))
		logDebug("value("+id+") = " + x)
		x
	}

	override def toString(): String = this.getClass.getSimpleName
}