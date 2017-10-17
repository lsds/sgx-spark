package org.apache.spark.sgx.iterator

object MsgIteratorReqHasNext extends Serializable {}
object MsgIteratorReqNext extends Serializable {}
object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator(fakeId: Long) extends Serializable {}