package org.apache.spark.sgx.broadcast

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxFactory

import scala.collection.mutable.HashMap

object SgxBroadcastEnclave extends Logging {
	private var com: SgxCommunicator = null

	def init(_com: SgxCommunicator): Unit = {
		logDebug("init")
		if (com != null) return
		com = _com
	}

	def value(id: Long): Any = {
		val x = com.sendRecv(new MsgBroadcastGetValue(id))
		logDebug("value("+id+") = " + x)
		x
	}

	override def toString(): String = this.getClass.getSimpleName
}