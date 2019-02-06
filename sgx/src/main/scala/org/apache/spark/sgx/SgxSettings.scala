/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sgx

object SgxSettings {
	val SGX_ENABLED = sys.env.get("SGX_ENABLED").getOrElse("false").toBoolean
	val USE_HDFS_ENCRYPTION = sys.env.get("USE_HDFS_ENCRYPTION").getOrElse("false").toBoolean

	val IS_ENCLAVE = sys.env.get("IS_ENCLAVE").getOrElse("false").toBoolean
	val IS_DRIVER = sys.env.get("IS_DRIVER").getOrElse("false").toBoolean
	val IS_WORKER = sys.env.get("IS_WORKER").getOrElse("false").toBoolean
	
	/*
	 * For debugging. Set this to false if the enclave side of Sgx-Spark does not run on
	 * sgx-lkl, but in a regular JVM on the host.
	 * Default: true
	 */
	val DEBUG_IS_ENCLAVE_REAL = sys.env.get("DEBUG_IS_ENCLAVE_REAL").getOrElse("true").toBoolean

	val CONNECTIONS = sys.env.get("CONNECTIONS").getOrElse("8").toInt
	val PREFETCH = sys.env.get("PREFETCH").getOrElse("1024").toInt

	val ENCRYPTION_KEY = sys.env.get("ENCRYPTION_KEY").getOrElse("0").toInt
	
	val BACKOFF_WAIT_MIN = sys.env.get("BACKOFF_WAIT_MIN").getOrElse("1").toInt
	val BACKOFF_WAIT_MAX = sys.env.get("BACKOFF_WAIT_MAX").getOrElse("64").toInt
	
	val SPARK_DEFAULT_BUFFER_SIZE = sys.env.get("SPARK_DEFAULT_BUFFER_SIZE").getOrElse("33554432").toInt
	
	/*
	 * Serializer to use.
	 * See Serialization.getSerializer() for valid options.
	 */
	val SERIALIZER = sys.env.get("SERIALIZER").getOrElse("commons");

	val SHMEM_FILE = sys.env.get("SGXLKL_SHMEM_FILE").getOrElse("")

	val SHMEM_SIZE = java.lang.Integer.decode(sys.env.get("SGXLKL_SHMEM_SIZE").getOrElse("1073741824"))

	val SHMEM_COMMON = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_COMMON").getOrElse("0")) // fail if not provided
	val SHMEM_OUT_TO_ENC = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_OUT_TO_ENC").getOrElse("0")) // fail if not provided
	val SHMEM_ENC_TO_OUT = java.lang.Long.decode(sys.env.get("SGXLKL_SHMEM_ENC_TO_OUT").getOrElse("0")) // fail if not provided
}
