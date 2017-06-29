package org.apache.spark.sgx

import java.net.InetAddress
import java.net.ServerSocket

import java.io.InputStream
import java.io.ObjectInputStream

import scala.util.control.Breaks.breakable
import scala.util.control.Breaks.break

import scala.collection.mutable.ListBuffer

class ObjectInputStreamWithCustomClassLoader(inputStream: InputStream) extends ObjectInputStream(inputStream) {
	override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
		try {
			Class.forName(desc.getName, false, getClass.getClassLoader)
		} catch {
			case ex: ClassNotFoundException => super.resolveClass(desc)
		}
	}
}

object SgxMain {
	def main(args: Array[String]): Unit = {

		val server = new ServerSocket(9999)
		while (true) {
			val sh = new SocketHelper(server.accept())

			// Receive SgxMapPartitionsRDDObject object and data objects
			val obj = sh.recvOne().asInstanceOf[SgxMapPartitionsRDDObject[Any,Any]]
			val data = sh.recvMany()

			// Apply function f()
			val it = obj.f(obj.partIndex, data)

			// Return the results
			sh.sendMany(it)

			sh.close()
		}

		server.close()
	}
}
