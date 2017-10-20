package org.apache.spark.sgx

import java.util.Base64

  /*
   * TODO: Encryption/Decryption are dummy operations.
   */

object Encrypt {
  def apply(plain: String, key: Long = 0): String = {
    val v = if (key <= 0) plain
    else {
      plain match {
        case _:String => ("x" * key.toInt) + plain + ("x" * key.toInt);
        case _:Any => plain
      }
    }
    Base64.getEncoder.encodeToString(Serialization.serialize(v))
  }
}

object Decrypt {
	def apply(enc: String, key: Long = 0): String = {
	    val v = if (key <= 0) enc else enc.substring(key.toInt, enc.length()-key.toInt)
    	Serialization.deserialize(Base64.getDecoder.decode(v)).asInstanceOf[String]
	}
}