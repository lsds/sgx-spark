package org.apache.spark.sgx

import org.apache.spark.internal.Logging

object SgxSettings extends Logging {
	val SGX_ENABLED = sys.env.get("SGX_ENABLED").getOrElse("false").toBoolean

	val IS_ENCLAVE = sys.env.get("IS_ENCLAVE").getOrElse("false").toBoolean
	val IS_DRIVER = sys.env.get("IS_DRIVER").getOrElse("false").toBoolean
	val IS_WORKER = sys.env.get("IS_WORKER").getOrElse("false").toBoolean

	val CONNECTIONS = sys.env.get("CONNECTIONS").getOrElse("1").toInt
	val PREFETCH = sys.env.get("PREFETCH").getOrElse("128").toInt

	val ENCRYPTION_KEY = sys.env.get("ENCRYPTION_KEY").getOrElse("0").toInt

	val SHMEM_FILE = {
		sys.env.get("SGXLKL_SHMEM_FILE").getOrElse({
			logError("SGXLKL_SHMEM_FILE not provided.")
			""})
	}

	val SHMEM_SIZE = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_SIZE").getOrElse("1073741824"))

	val SHMEM_OUT_TO_ENC = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_OUT_TO_ENC").getOrElse("0")) // fail if not provided
	val SHMEM_ENC_TO_OUT = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_ENC_TO_OUT").getOrElse("0")) // fail if not provided

	val SGX_USE_SHMEM = sys.env.get("SGX_USE_SHMEM").getOrElse("true").toBoolean
}
