//package org.apache.spark.sgx.sockets
//
//import org.apache.spark.internal.Logging
//import com.google.common.net.InetAddresses
//
//object SocketEnv extends Logging {
//	def getIpFromEnvVar(envvar: String): String = {
//		val addr = sys.env.get(envvar).getOrElse("")
//		if (!InetAddresses.isInetAddress(addr)) {
//			logWarning("Environment variable " + envvar + " undefined.")
//		}
//		addr
//	}
//
//	def getPortFromEnvVar(envvar: String): Int = {
//		val port = sys.env.get(envvar).getOrElse("0").toInt
//		if (port <= 0 || port > 65536) {
//			logWarning("Environment variable " + envvar + " undefined.")
//		}
//		port
//	}
//}