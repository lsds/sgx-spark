package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.ClientHandle
import org.apache.spark.sgx.IdentifierManager
import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.SgxFct
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.Encrypted

import org.apache.spark.util.collection.WritablePartitionedIterator
import org.apache.spark.storage.DiskBlockObjectWriter

import org.apache.spark.internal.Logging

class SgxFakePairIndicator() extends Serializable {}

case class SgxFakeIteratorException(id: Long) extends RuntimeException("A FakeIterator is just a placeholder and not supposed to be used. (" + id + ")") {}

private object FakeIterators extends IdentifierManager[Iterator[Any]]() {}

case class SgxFakeIterator[T](@transient val delegate: Iterator[T]) extends Iterator[T] with SgxIterator[T] with SgxIteratorIdentifier[T] with Logging {

	val id = scala.util.Random.nextLong

	FakeIterators.put(id, delegate)

	override def hasNext: Boolean = throw SgxFakeIteratorException(id)

	override def next: T = throw SgxFakeIteratorException(id)

	def access(): Iterator[T] = new SgxIteratorConsumer(ClientHandle.sendRecv[SgxIteratorProviderIdentifier[T]](MsgAccessFakeIterator(this)))

	def provide() = SgxFactory.newSgxIteratorProvider[Any](FakeIterators.remove(id), true).getIdentifier

	override def getIdentifier = this

	override def getIterator(context: String) = {
	  new Iterator[T] with Logging {

	    private val iter = FakeIterators.remove[Iterator[T]](id)
	    private var _type = -1
	    
	    if (iter == null) {
	      throw new IllegalStateException("Iterator does not exist. Has it been used before?")
	    }

	    override def hasNext: Boolean = iter.hasNext
	    
	    override def next = {
	      val n = iter.next
	      
	      if (_type == -1) {
          _type =
  	        if (n != null && n.isInstanceOf[Product2[Any,Any]] && n.asInstanceOf[Product2[Any,Any]]._2.isInstanceOf[SgxFakePairIndicator]) 1
  	        else 0
	      }
	      
    	  _type match {
	        case 1 =>
	          n.asInstanceOf[Product2[Encrypted,SgxFakePairIndicator]]._1.decrypt[T]
	        case default =>
            n
	      }
	    }
	  }
	}

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}
