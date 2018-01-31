package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast

abstract class MsgBroadcast[R] extends Serializable {
	def apply(): R
}

case class MsgBroadcastDestroy[T](bc: Broadcast[T], blocking: Boolean) extends MsgBroadcast[Unit] {
	override def apply = bc.destroy(blocking)
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

case class MsgBroadcastUnpersist[T](bc: Broadcast[T], blocking: Boolean) extends MsgBroadcast[Unit] {
	override def apply = bc.unpersist(blocking)
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

case class MsgBroadcastValue[T](bc: Broadcast[T]) extends MsgBroadcast[T] {
	override def apply = bc.value
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ")"
}

object MsgBroadcastReqClose extends Serializable {}