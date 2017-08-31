//package org.apache.spark.sgx
//
//import java.util.Base64
//
//object Encoding {
//  
//  def encode(value: Any): String = {
//    Base64.getEncoder.encodeToString(Serialization.serialize(value))
//  }
//  
//  def decode(value: String): Any = {
//    Serialization.deserialize(Base64.getDecoder.decode(value))
//  }
//}