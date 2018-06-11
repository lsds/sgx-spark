package org.apache.spark.sgx

import org.apache.spark.internal.Logging

abstract class SgxMessage[R] extends Serializable with Logging {
	final def send(): R = {
		logDebug(this + ".send()");
		val x = ClientHandle.sendRecv[R](this)
		logDebug(this + ".send() returned: " + x);
		x
	}

	def execute(): R

	override def toString = this.getClass.getSimpleName + "()"
}
