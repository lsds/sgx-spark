package org.apache.spark.sgx

import java.util.Base64

  /*
   * TODO: Encryption/Decryption are dummy operations.
   */

trait Encrypted extends Serializable {
	def decrypt: Any
}

private class EncryptedObj[T](cipher: T, dec: T => Any) extends Encrypted {
	def decrypt = dec(cipher)
}

object Encrypt {
	def apply(plain: Any): Encrypted = Base64StringEncrypt(plain)
}

private object Base64StringEncrypt {
	def apply(plain: Any): Encrypted = {
		new EncryptedObj[String](
			Base64.getEncoder.encodeToString(Serialization.serialize(plain)),
			(x: String) => Serialization.deserialize(Base64.getDecoder.decode(x))
			)
	}
}