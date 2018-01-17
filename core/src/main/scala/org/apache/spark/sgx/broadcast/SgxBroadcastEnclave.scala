package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.SgxCommunicator

object SgxBroadcastEnclave extends Logging {
	private var com: SgxCommunicator = null

	def init(_com: SgxCommunicator): Unit = {
		com = _com
	}

	def value[T](bc: Broadcast[T]): T = {
		com.sendRecv[T](new MsgBroadcastValue(bc))
	}

	def unpersist[T](bc: Broadcast[T], blocking: Boolean): Unit = {
		com.sendRecv[T](new MsgBroadcastUnpersist(bc, blocking))
	}

	override def toString(): String = this.getClass.getSimpleName
}