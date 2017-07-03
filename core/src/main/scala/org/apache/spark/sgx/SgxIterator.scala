package org.apache.spark.sgx

import java.net.ServerSocket

//
//import java.io.ObjectInputStream
//import java.io.ObjectOutputStream
//
//import java.net.Socket
//
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext
import java.net.InetAddress
import java.net.Socket

//
//import scala.collection.AbstractIterator
//import java.net.ServerSocket
//import java.net.InetAddress
//
object SgxMsgIteratorReqHasNext extends SgxMsg("iterator.req.hasNext") {}
object SgxMsgIteratorReqNext extends SgxMsg("iterator.req.next") {}
object SgxMsgIteratorReqClose extends SgxMsg("iterator.req.close") {}

//
///**
// * Note by FK: Currently not in use. May be useful at some later point.
// */
//
//class SgxIteratorStub[T](oos: ObjectOutputStream, ois: ObjectInputStream) extends AbstractIterator[T] {
//
//	def this(oos: ObjectOutputStream, ois: ObjectInputStream, x: Int)  {
//		this(oos, ois)
//		println("Constructor")
//	}
//
//	override def hasNext: Boolean = {
//		println("hasNext()?")
//		oos.writeObject(SgxMsgIteratorReqHasNext)
//		oos.flush
//		val x = ois.readObject().asInstanceOf[Boolean]
//		println(" -> " + x + " (" + x.getClass().getName + ")")
//		x
//	}
//
//	override def next: T = {
//		println("next()?")
//		oos.writeObject(SgxMsgIteratorReqNext)
//		oos.flush
//		val x = ois.readObject().asInstanceOf[T]
//		println(" -> " + x + " (" + x.getClass().getName + ")")
//		x
//	}
//}
//
//
//class SgxIteratorReal[T](it: Iterator[T], oos: ObjectOutputStream, ois: ObjectInputStream) extends Runnable {
//
//	def run = {
//		println("Running SgxIteratorReal")
//		breakable {
//			while(true) {
//				println("Waiting ... ")
//				ois.readObject() match {
//					case SgxMsgIteratorReqHasNext =>
//						val x = it.hasNext
//						println("next()? " + x + "(" + x.getClass().getName + ")")
//						oos.writeObject(x)
//						oos.flush
//						if (!it.hasNext) break
//					case SgxMsgIteratorReqNext =>
//						val x = it.next
//						println("next()? " + x + "(" + x.getClass().getName + ")")
//						oos.writeObject(x)
//						oos.flush
//					case x: Any =>
//						println("Unknown: " + x + "(" + x.getClass().getName + ")")
//				}
//				println("loop done")
//			}
//		}
//
//		println("break")
//	}
//}

class SgxIteratorServerIdentifier(host: String, port: Int) extends Serializable {
	def getHost(): String = { host }
	def getPort(): Int = { port }
	override def toString() = {
		s"SgxIteratorServerIdentifier($host,$port)"
	}
}

class SgxIteratorServer[T](context: TaskContext, delegate: Iterator[T], myport: Int) extends InterruptibleIterator[T](context, delegate) with Runnable {

	val identifier = new SgxIteratorServerIdentifier("localhost", myport)

	override def next(): T = {
//		println(s"SgxIteratorServer($myport): Regular next() call")
//		super.next()
		throw new RuntimeException(s"Access this iterator via port $myport")
	}

	override def hasNext(): Boolean = {
//		println(s"SgxIteratorServer($myport): Regular hasNext() call")
//		super.hasNext
		throw new RuntimeException(s"Access this iterator via port $myport")
	}

	def run = {
		println(s"Running SgxIteratorServer on port $myport")
		val sh = new SocketHelper(new ServerSocket(myport).accept())
		breakable {
			while(true) {
				sh.recvOne() match {
					case SgxMsgIteratorReqHasNext =>
						val x = super.hasNext
						println(s"SgxIteratorServer($myport).hasNext() -> " + x)
						sh.sendOne(x)
						if (!super.hasNext) break
					case SgxMsgIteratorReqNext =>
						val x = super.next
						println(s"SgxIteratorServer($myport).next() -> " + x)
						sh.sendOne(x)
					case SgxMsgIteratorReqClose =>
						break
					case x: Any =>
						println(s"SgxIteratorServer($myport).UNKNOWN() -> " + x + "(" + x.getClass().getName + ")")
				}
			}
		}

		println(s"Stopping SgxIteratorServer on port $myport")
		sh.close()
	}
}

class SgxIteratorServerBinding[T](itServer: SgxIteratorServer[T], theirHost: String, theirPort: Int) extends InterruptibleIterator[T](itServer.context, itServer.delegate) {
	def getTheirHost(): String = { theirHost }
	def getTheirPort(): Int = { theirPort }
	def getItServer(): SgxIteratorServer[T] = { itServer }
}



class SgxIteratorClient[T](id: SgxIteratorServerIdentifier) extends Iterator[T] {

	println(s"Connecting to remote iterator $id")
	val sh = new SocketHelper(new Socket(InetAddress.getByName(id.getHost()), id.getPort()))

	override def hasNext: Boolean = {
		sh.sendOne(SgxMsgIteratorReqHasNext)
		val x = sh.recvOne().asInstanceOf[Boolean]
		println(s"$id.hasNext() -> $x")
		x
	}

	override def next: T = {
		sh.sendOne(SgxMsgIteratorReqNext)
		val x = sh.recvOne().asInstanceOf[T]
		println(s"$id.next() -> " + x)
		x
	}

	def close() = {
		sh.sendOne(SgxMsgIteratorReqClose)
	}
}

