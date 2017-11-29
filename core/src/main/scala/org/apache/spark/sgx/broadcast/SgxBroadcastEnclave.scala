package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.sgx.SgxCommunicator

object SgxBroadcastEnclave {
	private var com: SgxCommunicator = null

	def init(_com: SgxCommunicator): Unit = {
		if (com != null) return
		com = _com
	}

	def value[T](bc: Broadcast[T]): T = {
		com.sendRecv[T](new MsgBroadcastGet(bc))
	}

	override def toString(): String = this.getClass.getSimpleName
}