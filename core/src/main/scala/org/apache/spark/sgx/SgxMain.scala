package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

class ObjectInputStreamWithCustomClassLoader(inputStream: InputStream) extends ObjectInputStream(inputStream) {
	override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
		try {
			Class.forName(desc.getName, false, getClass.getClassLoader)
		} catch {
			case ex: ClassNotFoundException => super.resolveClass(desc)
		}
	}
}

class SgxTask(obj: SgxFirstTask[Any,Any]) {
	def run(): Iterator[Any] = {
		println("Starting new SgxTask with remote iterator " + obj.id + ".")

		val it = new SgxIteratorClient[Any](obj.id)
		obj.f(obj.partIndex, it)
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {

		val server = new ServerSocket(9999)
		while (true) {
			val sh = new SocketHelper(server.accept())

			val itdesc = sh.recvOne().asInstanceOf[SgxFirstTask[Any,Any]]
			println("Starting task")

			val future = Future {
				new SgxTask(itdesc).run()
			}

			val it = Await.result(future, Duration.Inf)
			println("Iterator is ready.")
			new Thread(new Runnable() {
				def run = {
					sh.sendMany(it)
					sh.close()
				}
			}).start()
			println("Started thread.")


//			val it = new SgxTask(itdesc).run()
//			sh.sendMany(it)
//
//			sh.close()
		}

		server.close()
	}
}

//object SgxMain {
//	def main(args: Array[String]): Unit = {
//
//		val server = new ServerSocket(9999)
//		while (true) {
//			val sh = new SocketHelper(server.accept())
//
//			// Receive SgxMapPartitionsRDDObject object and data objects
//			val obj = sh.recvOne().asInstanceOf[SgxMapPartitionsRDDObject[Any,Any]]
//			val list = sh.recvMany()
//
//			// Apply function f()
//			val newit = obj.f(obj.partIndex, list)
//
//			// Return the results
//			sh.sendMany(newit)
//
//			sh.close()
//		}
//
//		server.close()
//	}
//}
