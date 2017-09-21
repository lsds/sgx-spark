package org.apache.spark.sgx

import java.util.Base64

class Encrypted[T](plainText: T, key: Long) extends Serializable {

//  private def doEncrypt[T](obj: T): T = {
//    obj match {
//      case s: String => s + "xxx"
//      case a: Any => a
//    }.asInstanceOf[T]
//  }
//
//  private def doDecrypt[T](obj: T): T = {
//    obj match {
//      case s: String => s.substring(0, s.length() - 3)
//      case a: Any => a
//    }.asInstanceOf[T]
//  }

  /*
   * TODO: Encryption/Decryption are dummy operations.
   */

  private val encVal = encrypt(plainText, key)

  private def encrypt(plain: T, key: Long = 0): String = {
    val v = if (key <= 0) plain
    else {
      plain match {
        case _:String => ("x" * key.toInt) + plain + ("x" * key.toInt);
        case _:Any => plain
      }
    }
    Base64.getEncoder.encodeToString(Serialization.serialize(v))
  }

  def decrypt(key: Long = 0): T = {
    val v = if (key <= 0) encVal
      else encVal.substring(key.toInt, encVal.length()-key.toInt)
    Serialization.deserialize(Base64.getDecoder.decode(v)).asInstanceOf[T]
  }
}