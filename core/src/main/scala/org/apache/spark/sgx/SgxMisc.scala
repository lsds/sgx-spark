package org.apache.spark.sgx

import java.io.{ ObjectOutputStream, ByteArrayOutputStream }

import org.apache.spark.internal.Logging

object SgxMisc extends Logging {
	def isSerializable(a: Any, s: String): Unit = {
		val str = "isSerializable(" + a.getClass.getName + "," + s + ")"
		try {
			new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(a);
			logDebug(str + " = true")
		} catch {
			case t @ (_: Throwable | _: Error) =>
				logDebug(str + " = false")
				logDebug(t.getStackTraceString)
		}
	}
}
