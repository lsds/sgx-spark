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

package org.apache.spark.examples

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object DefPartitioner extends Logging {

  def main(args: Array[String]) {

    try  {
	    val conf = new SparkConf().setAppName("DefPartitioner")

	    val sc = new SparkContext(conf)
			logDebug("main1")
			val x = List(('a', 1), ('a', 1), ('b', 1))
			logDebug("main2: " + x)
      val y = sc.parallelize(x)
      logDebug("main3: " + y.collect().mkString(","))
      logDebug("main3: " + y.count())
      
      val b = y.mapPartitionsWithIndex( (index: Int, it: Iterator[(Char,Int)]) =>it.toList.map(x => if (index ==0) {x}).iterator).collect
      logDebug("main4: " + b.mkString(","))
      
      
      val z = y.reduceByKey {
      case (x, y) => x + y
    }


	    
      logDebug("main5: " + z.count())
      val a = z.collect()
      logDebug("main6: " + a)
      // scalastyle:off
      a.foreach(x => println(x))
      

    } catch {
    	case e: Exception => logDebug(e.getMessage + "\n" + e.getStackTraceString)
    }

  }
}
