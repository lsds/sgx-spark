package org.apache.spark.sgx

import java.util.Base64
import org.apache.spark.internal.Logging

  /*
   * TODO: Encryption/Decryption are dummy operations.
   */

trait Encrypted extends Serializable {
	def decrypt: Any
}

trait Encryptable extends Serializable {
	def encrypt: Encrypted
}

private class EncryptedObj[T](cipher: T, dec: T => Any) extends Encrypted with Logging {
	def decrypt = dec(cipher)
}

object Encrypt {
	def apply(plain: Any): Encrypted = Base64StringEncrypt(plain)
}

private object Base64StringEncrypt extends Logging {

	def apply(plain: Any): Encrypted = {
		plain match {
			case e: Encryptable => e.encrypt
			case p: Any =>
				new EncryptedObj[String](
					Base64.getEncoder.encodeToString(Serialization.serialize(plain)),
					(x: String) => Serialization.deserialize(Base64.getDecoder.decode(x))
				)
		}
	}
}