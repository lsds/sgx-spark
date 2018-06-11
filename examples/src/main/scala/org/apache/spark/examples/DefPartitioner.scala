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
      case (x, y) => {
        x + y
      }
    }


	    
      logDebug("main5: " + z.count())
      val a = z.collect()
      logDebug("main6: " + a)
      a.foreach(x => println(x))
      

    } catch {
    	case e: Exception => logDebug(e.getMessage + "\n" + e.getStackTraceString)
    }

  }
}
