package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxCommunicator

trait SgxIteratorProviderIdentifier extends Serializable {
	def connect(): SgxCommunicator
}