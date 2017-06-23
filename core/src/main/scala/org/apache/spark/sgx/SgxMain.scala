package org.apache.spark.sgx

import java.net._
import java.io._
import scala.io._

class ObjectInputStreamWithCustomClassLoader(inputStream: InputStream) extends ObjectInputStream(inputStream) {
	override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
		println("  resolve: " + desc.getName + "(" + desc.getFields + ")")
		try {
			Class.forName(desc.getName, false, getClass.getClassLoader)
		} catch {
			case ex: ClassNotFoundException => super.resolveClass(desc)
		}
	}

	def myReadObject(): Option[_] = {
		try {
			Some(readObject())
		} catch {
        	case e: Exception => None
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

			println(" start resolving")
			val obj = ois.myReadObject()
//			if (obj.isEmpty) {
//				println(obj.asInstanceOf[Function[(Int, Iterator[_]), Iterator[_]]])
//			}


			println(" end resolving")
			println("Rcv object: " + obj.getOrElse("NONE") + "(" + obj.getOrElse("NONE").getClass().getName + ")")

			val reply = SgxReply("ack")
			oos.writeObject(reply)
			println("Snd  reply: " + reply)

			socket.close()
		}

		server.close()
	}
}

