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

private class EncryptedObj[T](cipher: T, dec: T => Any) extends Encrypted {
	def decrypt = dec(cipher)
}

object Encrypt {
	def apply(plain: Any): Encrypted = Base64StringEncrypt(plain)
}

private object Base64StringEncrypt extends Logging {
	def apply(plain: Any): Encrypted = {
		logDebug("Encrypting: " + plain)
		plain match {
			case e: Encryptable => e.encrypt
			case t: Tuple2[_,_] => new EncryptedTuple2(Encrypt(t._1), Encrypt(t._2))
			case p: Any =>
				new EncryptedObj[String](
					Base64.getEncoder.encodeToString(Serialization.serialize(plain)),
					(x: String) => Serialization.deserialize(Base64.getDecoder.decode(x))
				)
		}
	}
}

class EncryptedTuple2[T1,T2](t1: Encrypted, t2: Encrypted) extends Product2[T1,T2] with Encrypted {
	def decrypt = (t1.decrypt,t2.decrypt)

	def _1 = t1.decrypt.asInstanceOf[T1]
	def _2 = t2.decrypt.asInstanceOf[T2]
	def canEqual(that: Any) = decrypt.canEqual(that)
}
