// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
// $example off$

object Simple extends Logging {
  def main(args: Array[String]): Unit = {
    try {

      val conf = new SparkConf().setAppName("DebugJob")
      val sc = new SparkContext(conf)

      val x = List(('a', 1), ('b', 1), ('a', 1))
      val y = sc.parallelize(x)
      val z = y.reduceByKey((x: Int, y: Int) => x + y)
      y.collect().foreach(x => println(x))

      sc.stop()
    }
    catch {
      case e => logDebug(e.getStackTraceString)
    }
  }
}

// scalastyle:on println