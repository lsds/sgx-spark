package org.apache.spark.sgx

import org.apache.spark.sgx.sockets.SocketEnv

object SgxSettings {
	val SGX_ENABLED = sys.env.get("SGX_ENABLED").getOrElse("true").toBoolean

	val ENCLAVE_IP = SocketEnv.getIpFromEnvVar("SPARK_SGX_ENCLAVE_IP")
	val ENCLAVE_PORT = SocketEnv.getPortFromEnvVar("SPARK_SGX_ENCLAVE_PORT")

	val HOST_IP = SocketEnv.getIpFromEnvVar("SPARK_SGX_HOST_IP")
	val HOST_PORT = SocketEnv.getPortFromEnvVar("SPARK_SGX_HOST_PORT")

	val RETRIES = 10
	val CONNECTIONS = sys.env.get("CONNECTIONS").getOrElse("8").toInt
	val PREFETCH = sys.env.get("PREFETCH").getOrElse("1").toInt

	val ENCRYPTION_KEY = sys.env.get("ENCRYPTION_KEY").getOrElse("0").toInt
}
