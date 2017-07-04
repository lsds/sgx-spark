package org.apache.spark.sgx

import java.net.InetAddress
import java.net.Socket
import scala.reflect.ClassTag
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.concurrent.Future



class SgxMsg(s : String)
	extends Serializable {
	override def toString = s"SgxMsg($s)"
}

class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {
	def compute(t: SgxSuperTask[U,T]): Iterator[U] = {
		val sh = new SocketHelper(new Socket(InetAddress.getByName("localhost"), 9999))
		sh.sendOne(t)
		val res = sh.recvOne()
		sh.close()
		res.asInstanceOf[Iterator[U]]
	}
}