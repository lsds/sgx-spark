package org.apache.spark.sgx

import org.apache.spark.internal.Logging

abstract class SgxExecuteInside[R] extends Serializable with Logging {
	def executeInsideEnclave(): R = {
		logDebug(this + ".executeInsideEnclave()");
		val x = ClientHandle.sendRecv[R](this)
		logDebug(this + ".executeInsideEnclave() returned: " + x);
		x
	}

	def apply(): R
}
