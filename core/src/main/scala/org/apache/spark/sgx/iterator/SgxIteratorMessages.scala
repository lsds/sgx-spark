package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxMessage
import org.apache.spark.sgx.RecordReaderMaps
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.IFillBuffer
import org.apache.spark.sgx.SgxMessage
import org.apache.spark.sgx.shm.RingBuffProducer
import org.apache.spark.sgx.shm.RingBuffConsumer

case class MsgIteratorReqNextN(num: Int) extends Serializable {}
object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator[T](fakeIt: SgxFakeIterator[T]) extends SgxMessage[SgxIteratorProvIdentifier[Any]] {
	override def execute() = fakeIt.provide()
}

case class SgxShmIteratorConsumerClose() extends Serializable {}

case class SgxShmIteratorConsumerFillBufferMsg(inDelimiter: Boolean) extends Serializable {
}

case class SgxShmIteratorConsumerFillBuffer(com: SgxCommunicator, reader: RingBuffConsumer, writer: RingBuffProducer) extends IFillBuffer with Serializable {
  
  def fillBuffer(inDelimiter: Boolean) = {
//    com.sendRecv[Int](new SgxShmIteratorConsumerFillBufferMsg(inDelimiter))
    writer.write(new SgxShmIteratorConsumerFillBufferMsg(inDelimiter))
    reader.read().asInstanceOf[Int]
  }
}