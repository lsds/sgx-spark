package org.apache.spark.sgx

import java.io.InputStream
import java.io.ObjectInputStream
import java.net.ServerSocket

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

class SgxTask(itId: SgxIteratorServerIdentifier, port: Int) extends Runnable {
	def run() = {
		val server = new ServerSocket(port)
		println(s"Starting new SgxTask on port $port. The corresponding remote iterator is $itId.")
		while (true) {
			val sh = new SocketHelper(server.accept())
		}
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {

		val server = new ServerSocket(9999)
		while (true) {
			val sh = new SocketHelper(server.accept())

			val itdesc = sh.recvOne().asInstanceOf[SgxIteratorServerIdentifier]

			val port = 40000 + scala.util.Random.nextInt(10000)
			new Thread(new SgxTask(itdesc, port)).start()
			sh.sendOne(port)

			sh.close()
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
