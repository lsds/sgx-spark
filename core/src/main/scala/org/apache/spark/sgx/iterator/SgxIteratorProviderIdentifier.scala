package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxCommunicationInterface

trait SgxIteratorProviderIdentifier extends Serializable {
	def connect(): SgxCommunicationInterface
}