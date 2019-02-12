/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sgx

import org.apache.spark.internal.Logging

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

object EncryptionAlgorithm extends Enumeration {
  val None, Base64 = Value
}

object Encrypt {
  
	def apply(plain: Any): Encrypted = {
	// Base64StringEncrypt(plain)
	  new NoEncryption(plain)
	}
}

private class NoEncryption(o: Any) extends Encrypted {
  def decrypt[U]: U = o.asInstanceOf[U]
}

private object Base64StringEncrypt extends Logging {
	def apply(plain: Any): Encrypted = {
		val x = plain match {
			case p: Any =>
				new EncryptedObj({
				  val a = Serialization.serialize(plain)
					val b = org.apache.commons.codec.binary.Base64.encodeBase64(a)
					b
					// val stream = new ByteArrayOutputStream()
					// val cos = new ObjectOutputStream(new CipherOutputStream(stream, new NullCipher))
					//  cos.writeObject(plain)
					// val r = stream.toByteArray()
					// cos.close()
					// r
				},
					(x: Array[Byte]) => {
					  val y = org.apache.commons.codec.binary.Base64.decodeBase64(x)
					  val z = Serialization.deserialize(y)
					  z
						// new ObjectInputStream(new CipherInputStream(new ByteArrayInputStream(x), new NullCipher)).readObject()
					}
				)
		}
		logDebug("Encryption result: " + x)
		x
	}
}

