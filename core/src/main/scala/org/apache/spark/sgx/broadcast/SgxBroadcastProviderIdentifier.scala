package org.apache.spark.sgx.broadcast

import org.apache.spark.sgx.SgxCommunicator

trait SgxBroadcastProviderIdentifier extends Serializable {
	def connect(): SgxCommunicator
}