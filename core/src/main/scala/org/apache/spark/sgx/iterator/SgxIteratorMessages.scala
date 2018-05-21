package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxMessage
import org.apache.spark.sgx.RecordReaderMaps

case class MsgIteratorReqNextN(num: Int) extends Serializable {}
object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator[T](fakeIt: SgxFakeIterator[T]) extends SgxMessage[SgxIteratorProvIdentifier[Any]] {
	override def execute() = fakeIt.provide()
}

case class SgxShmIteratorConsumerClose(id: Long) extends SgxMessage[Unit] {
  
  logDebug("Creating " + this)
  
  override def execute() = RecordReaderMaps.get(id).close()
}