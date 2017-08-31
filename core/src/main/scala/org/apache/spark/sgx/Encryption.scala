package org.apache.spark.sgx

import java.util.Base64

object Encryption {
  
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
  
  def encrypt(value: Any, key: Long = 0): String = {
    val v = if (key <= 0) value
    else {
      value match {
        case _:String => ("x" * key.toInt) + value + ("x" * key.toInt);
        case _:Any => value
      }
    }
    Base64.getEncoder.encodeToString(Serialization.serialize(v))
  }
  
  def decrypt(value: String, key: Long = 0): Any = {
    val v = if (key <= 0) value
    else {
      value.substring(key.toInt, value.length()-key.toInt)
    }
    Serialization.deserialize(Base64.getDecoder.decode(v))
  }
}