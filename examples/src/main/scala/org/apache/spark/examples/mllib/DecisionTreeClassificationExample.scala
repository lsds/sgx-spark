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

// scalastyle:off println
package org.apache.spark.examples.mllib

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

object DecisionTreeClassificationExample extends Logging {

    def parseLibSVMRecord(line: String): (Double, Array[Int], Array[Double]) = {
      val items = line.split(' ')
      val label = items.head.toDouble
      val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toDouble
        (index, value)
      }.unzip

      var previous = -1
      var i = 0
      val indicesLength = indices.length
      while (i < indicesLength) {
        val current = indices(i)
        require(current > previous, s"indices should be one-based and in ascending order;"
          + s""" found current=$current, previous=$previous; line="$line"""")
        previous = current
        i += 1
      }
      (label, indices.toArray, values.toArray)
    }

    def atomicFoo(line: String): String = {
      line + " hi!"
    }

  def main(args: Array[String]): Unit = {
    try {

      val conf = new SparkConf().setAppName("DecisionTreeClassificationExample")
      val sc = new SparkContext(conf)

      // val x = List(('a', 1), ('b', 1))
      // val y = sc.parallelize(x)
      // println(y.collectAsMap)

      // $example on$
      // Load and parse the data file.
      // val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
      val data = sc.textFile(args(0)).map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
      println("\n\n\n\n LOADED, TRIMMED AND FILTERED TEXT FILE \n\n\n\n")

      val parsedData = data.map(parseLibSVMRecord)
      println("\n\n\n\n PARSED THE DATA \n\n\n\n")
      //
      val d = parsedData.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
      println("\n\n\n\n COUNTED THE NUMBER OF FEATURES \n\n\n\n")
      println(d)
      //
      val processedData = parsedData.map { case (label, indices, values) =>
        LabeledPoint(label, Vectors.sparse(d, indices, values))
      }
      println("\n\n\n\n PROCESSED DATA INTO LABELLED POINTS \n\n\n\n")
      //
      //
      // // Split the data into training and test sets (30% held out for testing)
      val splits = processedData.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))
      println("\n\n\n\n SPLIT DATA INTO TRAINING AND TEST SETS \n\n\n\n")
      println(trainingData.count)
      println(testData.count)
      //
      //
      // // Train a DecisionTree model.
      // //  Empty categoricalFeaturesInfo indicates all features are continuous.
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val impurity = "gini"
      val maxDepth = 5
      val maxBins = 32
      // //
      val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        impurity, maxDepth, maxBins)
      println("\n\n\n\n CREATED DECISION TREE CLASSIFIER \n\n\n\n")

      // Evaluate model on test instances and compute test error
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
      logDebug("decision tree done")
      println("\n\n\n\n FINISHED FITTING TREE \n\n\n\n")
      println(s"Test Error = $testErr")
      println(s"Learned classification tree model:\n ${model.toDebugString}")

      // Save and load model
      model.save(sc, args(1))
      // val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
      // $example off$

      sc.stop()
    }
    catch {
      case e => logDebug(e.getStackTraceString)
    }
  }
}

// scalastyle:on println
