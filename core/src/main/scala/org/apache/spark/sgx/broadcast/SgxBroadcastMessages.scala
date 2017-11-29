package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast

case class MsgBroadcastGet[T](bc: Broadcast[T]) extends Serializable {
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ")"
}

object MsgBroadcastReqClose extends Serializable {}