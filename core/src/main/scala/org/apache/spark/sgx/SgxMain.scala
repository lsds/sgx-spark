package org.apache.spark.sgx

import java.net.InetAddress
import java.net.ServerSocket

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.util.Try

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
			val socket = server.accept()
			val oos = new ObjectOutputStream(socket.getOutputStream())
			val ois = new ObjectInputStreamWithCustomClassLoader(socket.getInputStream())

			// Read the object provided
			val obj : SgxMapPartitionsRDDObject[Any,Any] = SocketHelper.read(ois).asInstanceOf[SgxMapPartitionsRDDObject[Any,Any]]
			println("reading: " + obj)

			var list = new ListBuffer[Any]()
			breakable {
				while(true) {
					SocketHelper.read(ois) match {
						case SgxDone => break
						case x: Any => {
							println("  Receiving: " + x + " (" + x.getClass.getName + ")")
							list += x
						}
					}
				}
			}
			println("  Received number of objects: " + list.size)

			val newit = obj.f(obj.partIndex, list.iterator)

			newit.foreach {
				x => println("Sending: " + x + " (" + x.getClass.getName + ")")
				SocketHelper.send(oos, x)
			}
			SocketHelper.send(oos, SgxDone)

			socket.close()
		}

		server.close()
	}
}
