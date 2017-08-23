package org.apache.spark.sgx

import java.net.ServerSocket

object SgxMain3 {
	def main(args: Array[String]): Unit = {
		val server = new ServerSocket(9999)
		println("Socket created")
		while(true) {
			println("While start")
			server.accept()
			println("While end")
		}
		println("Main end")
	}
}


