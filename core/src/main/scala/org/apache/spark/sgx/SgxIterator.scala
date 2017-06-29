package org.apache.spark.sgx

import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import java.net.Socket

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import scala.collection.AbstractIterator

object SgxMsgIteratorReqHasNext extends SgxMsg("iterator.req.hasNext") {}
object SgxMsgIteratorReqNext extends SgxMsg("iterator.req.next") {}


/**
 * Note by FK: Currently not in use. May be useful at some later point.
 */

class SgxIteratorStub[T](oos: ObjectOutputStream, ois: ObjectInputStream) extends AbstractIterator[T] {

	def this(oos: ObjectOutputStream, ois: ObjectInputStream, x: Int)  {
		this(oos, ois)
		println("Constructor")
	}

	override def hasNext: Boolean = {
		println("hasNext()?")
		oos.writeObject(SgxMsgIteratorReqHasNext)
		oos.flush
		val x = ois.readObject().asInstanceOf[Boolean]
		println(" -> " + x + " (" + x.getClass().getName + ")")
		x
	}

	override def next: T = {
		println("next()?")
		oos.writeObject(SgxMsgIteratorReqNext)
		oos.flush
		val x = ois.readObject().asInstanceOf[T]
		println(" -> " + x + " (" + x.getClass().getName + ")")
		x
	}
}


class SgxIteratorReal[T](it: Iterator[T], oos: ObjectOutputStream, ois: ObjectInputStream) extends Runnable {

	def run = {
		println("Running SgxIteratorReal")
		breakable {
			while(true) {
				println("Waiting ... ")
				ois.readObject() match {
					case SgxMsgIteratorReqHasNext =>
						val x = it.hasNext
						println("next()? " + x + "(" + x.getClass().getName + ")")
						oos.writeObject(x)
						oos.flush
						if (!it.hasNext) break
					case SgxMsgIteratorReqNext =>
						val x = it.next
						println("next()? " + x + "(" + x.getClass().getName + ")")
						oos.writeObject(x)
						oos.flush
					case x: Any =>
						println("Unknown: " + x + "(" + x.getClass().getName + ")")
				}
				println("loop done")
			}
		}

		println("break")
	}
}