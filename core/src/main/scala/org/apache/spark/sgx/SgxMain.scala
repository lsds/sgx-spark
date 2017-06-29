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
			val obj : SgxMapPartitionsRDDObject[Any,Any] = ois.readUnshared().asInstanceOf[SgxMapPartitionsRDDObject[Any,Any]]
			println("reading: " + obj)

			var list = new ListBuffer[Any]()
			breakable {
				while(true) {
					ois.readUnshared() match {
						case SgxDone => break
						case x: Any => {
							println("  Receiving: " + x + " (" + x.getClass.getName + ")")
							list += x
						}
					}
				}
			}
			println("  Received number of objects: " + list.size)

			println("before f()")
			val newit = obj.f(obj.partIndex, list.iterator)
			println("after f(): " + newit)

			newit.foreach {
				x => println("Sending: " + x + " (" + x.getClass.getName + ")")
				oos.writeUnshared(x)
			}
			oos.writeUnshared(SgxDone)
			oos.flush()

			socket.close()
		}

		server.close()
	}
}
