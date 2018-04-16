package org.apache.spark.sgx

import java.util.Base64
import java.io.Externalizable
import java.io.ObjectInput
import java.io.ObjectOutput
import org.apache.spark.internal.Logging

import java.io.ByteArrayOutputStream
import javax.crypto.CipherOutputStream
import javax.crypto.Cipher
import javax.crypto.NullCipher
import java.io.ByteArrayInputStream
import javax.crypto.CipherInputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

  /*
   * TODO: Encryption/Decryption are dummy operations.
   */

trait Encrypted extends Serializable {
	def decrypt[U]: U

}

private class EncryptedObj(private var cipher: Array[Byte], private var dec: Array[Byte] => Any) extends Encrypted {
  
  def this() = this(null.asInstanceOf[Array[Byte]], null)
  
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
		val x = plain match {
			case p: Any =>
				new EncryptedObj({
				  logDebug("encrypt0 " + plain)
				  val a = Serialization.serialize(plain)
				  logDebug("encrypt1("+a.length+") " + java.util.Arrays.hashCode(a))
					val b = Base64.getEncoder.encode(a)
					logDebug("encrypt2("+b.length+") ")
					b
//				  val stream = new ByteArrayOutputStream()
//				  val cos = new ObjectOutputStream(new CipherOutputStream(stream, new NullCipher))
//				  cos.writeObject(plain)
//				  val r = stream.toByteArray()
//				  cos.close()
//				  r
				},
					(x: Array[Byte]) => {
					  logDebug("decrypt0("+x.length+") ")
					  val y = Base64.getDecoder.decode(x)
					  logDebug("decrypt1("+y.length+") " + java.util.Arrays.hashCode(y))
					  val z = Serialization.deserialize(y)
					  logDebug("decrypt2 " + z)
					  z
					  
//					  new ObjectInputStream(new CipherInputStream(new ByteArrayInputStream(x), new NullCipher)).readObject()
					}
				)
		}
		logDebug("Encryption result: " + x)
		x
	}
}

