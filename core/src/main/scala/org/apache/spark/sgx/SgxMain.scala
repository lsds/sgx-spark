package org.apache.spark.sgx

import java.net._
import java.io._
import scala.io._

object SgxMain {
	def main(args: Array[String]): Unit = {
		val pw = new PrintWriter(new File("/tmp/hello.txt" ))
		pw.println("Enclave code")

		val server = new ServerSocket(9999)
		while (true) {
			val s = server.accept()
			val in = new BufferedSource(s.getInputStream()).getLines()
			val out = new PrintStream(s.getOutputStream)

			pw.println(in.next())
			out.println("ack")
			pw.flush()
			s.close()
		}

		pw.close()
	}
}

