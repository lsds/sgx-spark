package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.SgxCommunicator

object SgxBroadcastEnclave extends Logging {
	private var com: SgxCommunicator = null

	def init(_com: SgxCommunicator): Unit = {
		com = _com
	}

	def destroy[T](bc: Broadcast[T], blocking: Boolean): Unit = {
		com.sendRecv[T](new MsgBroadcastDestroy(bc, blocking))
	}

	def unpersist[T](bc: Broadcast[T], blocking: Boolean): Unit = {
		com.sendRecv[T](new MsgBroadcastUnpersist(bc, blocking))
	}

	def value[T](bc: Broadcast[T]): T = {
		com.sendRecv[T](new MsgBroadcastValue(bc))
	}

	override def toString(): String = this.getClass.getSimpleName
}

abstract class MsgBroadcast[R] extends Serializable {
	def apply(): R
}

private case class MsgBroadcastDestroy[T](bc: Broadcast[T], blocking: Boolean) extends MsgBroadcast[Unit] {
	override def apply = bc.destroy(blocking)
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

private case class MsgBroadcastUnpersist[T](bc: Broadcast[T], blocking: Boolean) extends MsgBroadcast[Unit] {
	override def apply = bc.unpersist(blocking)
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

private case class MsgBroadcastValue[T](bc: Broadcast[T]) extends MsgBroadcast[T] {
	override def apply = bc.value
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ")"
}

object MsgBroadcastReqClose extends Serializable {}