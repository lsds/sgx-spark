package org.apache.spark.sgx

import java.net._
import java.io._
import scala.io._

import scala.util.Try

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
			val socket = server.accept()
			val oos = new ObjectOutputStream(socket.getOutputStream())
			val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

			// Read the object provided
			val obj : SgxMapPartitionsRDDObject[Any,Any] = ois.readObject().asInstanceOf[SgxMapPartitionsRDDObject[Any,Any]]

			val x = obj.f(obj.partIndex, List("a","b","c","a").iterator)
			try {
				x.foreach { a => Try(println(a)) }
			} catch {
				case e: ClassCastException => None
			}

			// Send ack
			oos.writeObject(SgxAck)

			socket.close()
		}

		server.close()
	}
}


