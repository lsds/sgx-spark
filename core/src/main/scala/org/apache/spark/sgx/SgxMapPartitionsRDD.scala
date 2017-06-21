package org.apache.spark.sgx

import scala.reflect.ClassTag

import java.net._
import java.io._
import scala.io._

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
		val s = new Socket(InetAddress.getByName("localhost"), 9999)

		println("a")

		val out = new PrintStream(s.getOutputStream())
		println("b")
		out.println("message to server")
		println("c")

		val in = new BufferedSource(s.getInputStream()).getLines()
		println("d")
		println("Received: " + in.next())
		println("e")

		s.close()

		f(partIndex, it)
	}
}