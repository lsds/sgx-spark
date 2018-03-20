package org.apache.spark.sgx

import java.util.Base64
import org.apache.spark.internal.Logging

  /*
   * TODO: Encryption/Decryption are dummy operations.
   */

trait Encrypted extends Serializable {
	def decrypt[U]: U
}

trait Encryptable extends Serializable {
	def encrypt: Encrypted
}

private class EncryptedObj[T](cipher: T, dec: T => Any) extends Encrypted {
	def decrypt[U]: U = {
		if (SgxSettings.IS_ENCLAVE) dec(cipher).asInstanceOf[U]
		else throw new RuntimeException("Must not decrypt outside of enclave")
	}
}

object Encrypt {
	def apply(plain: Any): Encrypted = Base64StringEncrypt(plain)
}

private object Base64StringEncrypt extends Logging {
	def apply(plain: Any): Encrypted = {
		logDebug("Encrypting: " + plain)
		val x = plain match {
			case e: Encryptable =>
				logDebug("Encryptable")
				e.encrypt
			case p: Any =>
				logDebug("EncryptedObj")
				new EncryptedObj[String](
					Base64.getEncoder.encodeToString(Serialization.serialize(plain)),
					(x: String) => Serialization.deserialize(Base64.getDecoder.decode(x))
				)
		}
		logDebug("Encryption result: " + x)
		x
	}
}

