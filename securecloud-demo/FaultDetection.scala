// project:
// H2020 - SecureCloud - Secure Big Data Processing in Untrusted Clouds
// https://www.securecloudproject.eu/
//
// authors:
// Florian Kelbert - f.kelbert@imperial.ac.uk
// Édson Takashi Yano - edson.yano@lactec.org.br


package org.apache.spark.examples.lactec

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{LogManager, Level}
import org.apache.commons.logging.LogFactory
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File

// Class that get values of Customers from TestCustomer.csv file
case class Customer(
		id: Long,
		meterHash: String,
		typ: String,
		local: String,
		contract: String,
		meter: String,
		model: String,
		client: String,
		latitude: Double,
		longitude: Double) {

	def this(a: Array[String]) =
		this(a(0).toLong, a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8).toDouble, a(9).toDouble)

	override def toString = s"${getClass.getSimpleName}($id, $meterHash, $typ, $local, $contract, $meter, $model, $client, $latitude, $longitude)"

}

// Class that get values of DSMs from TestDsm.csv file
case class Dsm(
	local: String,
  	name: String) {

    	def this(d: Array[String]) =  this(d(0), d(1))
}

// Class that get values of faults from TestFaults.csv file
case class Fault(
  	id: Long,
	customer_id: Long,
	date: String,
	time: String,
	length_time: String) {
	
	def this(f: Array[String]) = this(f(0).toLong, f(1).toLong, f(2), f(3), f(4))
}

object FaultDetection extends Logging {

  def main(args: Array[String]) {

	LogManager.getRootLogger().setLevel(Level.DEBUG)
	val log = LogFactory.getLog("LOG:")

	val conf = new SparkConf().setAppName("Lactec fault detection")
	val sc = new SparkContext(conf)

  val initStartTime = System.nanoTime()

	// First PairRDD based in TestCustomer.csv file, with (local) as key and (id, client, latitude, contract, meter) as value
	val pairRDDC = sc.textFile(args(0))
			.map(line => new Customer(line.split(";")))
			.filter(c => c.longitude != 0 && c.latitude != 0)
			.keyBy(c => c.local)
			.mapValues(c => (c.id, c.client, c.latitude, c.longitude, c.contract, c.meter))

	
			
	// Second PairRDD based in TestDsm.csv file, with (local) as key and (name) as value
	// mapPartitionsWithIndex it's being used considering that the file has got a header
        val pairRDDD = sc.textFile(args(1))
			.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } 
			.map(line => new Dsm(line.split(";")))
			.keyBy(d => d.local)
			.mapValues(d => d.name)
	

	// First Join - Join Between pairRDD of Customer and pairRDD of Dsm. 
	// The result will be an Array structured by ( id, (client, latitude, longitude, contract, meter, local, name))
	val pairRDDJoin1 = pairRDDC.join(pairRDDD)
        .map{ case ((local), ((id, client, latitude, longitude, contract, meter), name)) => (id, client, latitude, longitude, contract, meter, local, name)}
        .keyBy(j => j._1)
        .mapValues(j => (j._2, j._3, j._4, j._5, j._6, j._7, j._8))
				

	// Third PairRDD based in TestFaults.csv file, with (customer_id) as key and (date, time, length_time) as value
	// mapPartitionsWithIndex it's being used considering that the file has got a header
	val pairRDDF = sc.textFile(args(2))
		.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
		.map(line => new Fault(line.split(";")))
		.filter(f => f.date >= args(3) && f.date <= args(4))
		.keyBy(f => f.customer_id)
		.mapValues(f => (f.date, f.time, f.length_time))

		
	// Second Join - Join Between First Join and pairRDD of Faults
	// Within of the first mapValues a function is created to return the total of seconds of faults
	val pairRDDJoin2 = pairRDDJoin1.join(pairRDDF)
				.keyBy(j => (j._1, j._2._1._1, j._2._1._2, j._2._1._3, j._2._1._4, j._2._1._5, j._2._1._6, j._2._1._7))
				.mapValues(j => ({ val time = j._2._2._3.split(":")
						  val hour = time(0).toInt * 3600
						  val minute = time(1).toInt * 60
						  val second = time(2).toDouble
						  val total = (hour + minute + second).toInt
						  (total)
						},1)
					)	
				.reduceByKey{
					case ((totL,countL), (totR,countR)) =>
						(totL + totR, countL + countR)
				}
				.mapValues{
					case (tot, count) => (count, tot)
				}


			pairRDDJoin2.saveAs(args(5))

      val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
      logInfo(f"Sgx-Spark job finished after $initTimeInSeconds%.3f seconds.")

    	sc.stop()
  }

}
