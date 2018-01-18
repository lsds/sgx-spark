package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast

case class MsgBroadcastDestroy[T](bc: Broadcast[T], blocking: Boolean) extends Serializable {
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

case class MsgBroadcastUnpersist[T](bc: Broadcast[T], blocking: Boolean) extends Serializable {
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

case class MsgBroadcastValue[T](bc: Broadcast[T]) extends Serializable {
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ")"
}

object MsgBroadcastReqClose extends Serializable {}