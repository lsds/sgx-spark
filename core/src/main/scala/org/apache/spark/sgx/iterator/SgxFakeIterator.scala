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

	// we need to allow hasNext, as it is being accessed
	// by Java's instanceof operator.
	// Will always return true. A potential subsequent call to next()
	// will then surely throw an Exception
	override def hasNext: Boolean = throw SgxFakeIteratorException(id)

	override def next: T = throw SgxFakeIteratorException(id)

	def access(): Iterator[T] = new SgxIteratorConsumer(ClientHandle.sendRecv[SgxIteratorProviderIdentifier[T]](MsgAccessFakeIterator(this)))

	def provide() = SgxFactory.newSgxIteratorProvider[Any](FakeIterators.remove(id), true).getIdentifier

	override def getIdentifier = this

	override def getIterator(context: String) = {
	  logDebug("aaa 110")
	  new Iterator[T] with Logging {

	    private val i = FakeIterators.remove[Iterator[T]](id)
	    if (i == null) {
	      throw new IllegalStateException("Iterator does not exist. Has it been used before?")
	    }
	    
	    private var _type = -1

	    override def hasNext: Boolean = i.hasNext
	      
	      
//	      try {
//	      if (!i.hasNext) {
//	        logDebug("aaa no next")
//	         Thread.currentThread().getStackTrace.foreach(x => logDebug("  " + x))
//	        false
//	      } else {
//  	      val n = i.next()
//  	      
//  	      if (_type == -1) {
//  	        _type =
//  	        if (n != null && n.isInstanceOf[Product2[Any,Any]] && n.asInstanceOf[Product2[Any,Any]]._2.isInstanceOf[SgxFakePairIndicator]) 1
//  	        else 0
//  	      }
//  	      logDebug("aaa 118: " + _type)
//  	      
//  	      _next = _type match {
//  	        case 1 =>
//  	          n.asInstanceOf[Product2[Encrypted,SgxFakePairIndicator]]._1.decrypt[T]
//  	        case default =>
//              n
//  	      }
//  	      
//  	      logDebug("aaa next prepared: " + n)
//  	      true
//	      }
//	      } catch {
//	        case e: Exception =>
//	          logDebug("Exception: " + e.getMessage)
//	          logDebug(e.getStackTraceString)
//	          throw new RuntimeException(e)
//	          false
//	      }
//	    }
	    
	    override def next = {
	      val _next = i.next
	      
	      if (_type == -1) {
          _type =
  	        if (_next != null && _next.isInstanceOf[Product2[Any,Any]] && _next.asInstanceOf[Product2[Any,Any]]._2.isInstanceOf[SgxFakePairIndicator]) 1
  	        else 0
	      }
	      
	  	   val x = _type match {
  	        case 1 =>
  	          _next.asInstanceOf[Product2[Encrypted,SgxFakePairIndicator]]._1.decrypt[T]
  	        case default =>
              _next
  	      }      
	      
	      logDebug("www next returning: " + x)
	      x
	    }
	  }
	}

	override def toString = this.getClass.getSimpleName + "(id=" + id + ")"
}
