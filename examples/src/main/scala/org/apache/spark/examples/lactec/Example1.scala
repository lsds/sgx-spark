package org.apache.spark.examples.lactec

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Record(
		id: Long,
		meterHash: String,
		typ: String,
		local: String,
		contract: Long,
		meter: Long,
		model: String,
		client: String,
		latitude: Double,
		longitude: Double) extends Serializable {

	def this(a: Array[String]) =
		this(a(0).toLong, a(1), a(2), a(3), a(4).toLong, a(5).toLong, a(6), a(7), a(8).toDouble, a(9).toDouble)

	override def toString = s"${getClass.getSimpleName}($id, $meterHash, $typ, $local, $contract, $meter, $model, $client, $latitude, $longitude)"

}

object Example1 extends Logging {

  def main(args: Array[String]) {

    try  {
	    val conf = new SparkConf().setAppName("Lactec example 1")

	    new SparkContext(conf)
			.textFile(args(0))
			.map(line => new Record(line.split(";")))
			.filter(r => r.longitude != 0 && r.latitude != 0)
			.sortBy(r => (r.client, r.contract, r.meter))
			.collect()
			.foreach(x => logDebug(x.toString()))


    } catch {
    	case e: Exception => logDebug(e.getMessage + "\n" + e.getStackTraceString)
    }

  }
}
