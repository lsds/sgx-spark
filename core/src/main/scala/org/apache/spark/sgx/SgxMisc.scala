//package org.apache.spark.sgx
//
//import java.io.{ObjectOutputStream,ByteArrayOutputStream}
//
//object SgxMisc {
//  def isSerializable(a: Any, s:String): Unit = {
//	  val str = "isSerializable("+a.getClass.getName+","+s+")"
//      try {
//	 	  new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(a);
//	 	  println(str + " = true")
//	  } catch {
//	 	  case t @ (_: Throwable | _: Error) =>
//	 	 	  println(str + " = false")
//	 	 	  println(t.getStackTraceString)
//	  }
//  }
//}