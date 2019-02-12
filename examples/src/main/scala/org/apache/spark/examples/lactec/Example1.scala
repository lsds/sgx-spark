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
  
  import scala.math.Ordered.orderingToOrdered

	def this(a: Array[String]) =
		this(a(0).toLong, a(1), a(2), a(3), a(4).toLong, a(5).toLong, a(6), a(7), a(8).toDouble, a(9).toDouble)

//	def compare(that: Record): Int = (this.client, this.contract, this.meter) compare (that.client, that.contract, that.meter)	
		
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
			.sortBy(r => (r.client, r.contract, r.meter), true)
			.collect()
			.foreach(x => logDebug(x.toString()))


    } catch {
    	case e: Exception => logDebug(e.getMessage + "\n" + e.getStackTraceString)
    }

  }
}
