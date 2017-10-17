package org.apache.spark.sgx

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.sockets.SocketEnv

object SgxSettings extends Logging {
	val SGX_ENABLED = sys.env.get("SGX_ENABLED").getOrElse("true").toBoolean

	val IS_ENCLAVE = sys.env.get("IS_ENCLAVE").getOrElse("false").toBoolean

	val ENCLAVE_IP = SocketEnv.getIpFromEnvVar("SPARK_SGX_ENCLAVE_IP")
	val ENCLAVE_PORT = SocketEnv.getPortFromEnvVar("SPARK_SGX_ENCLAVE_PORT")

	val HOST_IP = SocketEnv.getIpFromEnvVar("SPARK_SGX_HOST_IP")
	val HOST_PORT = SocketEnv.getPortFromEnvVar("SPARK_SGX_HOST_PORT")

	val RETRIES = 10
	val CONNECTIONS = sys.env.get("CONNECTIONS").getOrElse("1").toInt
	val PREFETCH = sys.env.get("PREFETCH").getOrElse("8").toInt

	val ENCRYPTION_KEY = sys.env.get("ENCRYPTION_KEY").getOrElse("0").toInt

	val SHMEM_FILE = {
		sys.env.get("SGXLKL_SHMEM_FILE").getOrElse({
			logError("SGXLKL_SHMEM_FILE not provided.")
			""})
	}

	val SHMEM_SIZE = java.lang.Integer.decode(sys.env.get("SGXLKL_SHMEM_SIZE").getOrElse("4096"))

	val SHMEM_OUT_TO_ENC = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_OUT_TO_ENC").getOrElse("0")) // fail if not provided
	val SHMEM_ENC_TO_OUT = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_ENC_TO_OUT").getOrElse("0")) // fail if not provided

	val SGX_USE_SHMEM = sys.env.get("SGX_USE_SHMEM").getOrElse("false").toBoolean
}
