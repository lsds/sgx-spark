package org.apache.spark.sgx.broadcast

case class MsgBroadcastGetValue(id: Long) extends Serializable {}
object MsgBroadcastReqClose extends Serializable {}