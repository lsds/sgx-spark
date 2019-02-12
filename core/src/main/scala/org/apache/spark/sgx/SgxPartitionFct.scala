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

package org.apache.spark.sgx

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ThreadUtils

object SgxPartitionFct {

	def defaultPartitioner(rddId: Int, otherIds: Int*) =
		new DefaultPartitioner(rddId, otherIds: _*).send()

	private abstract class SgxTaskPartitioner extends SgxMessage[Partitioner] {
		override def toString = this.getClass.getSimpleName
	}

	private case class DefaultPartitioner(rddId: Int, otherIds: Int*) extends SgxTaskPartitioner {
		def execute() = ThreadUtils.awaitResult( Future {
			val rdd = SgxMain.rddIds.get(rddId).asInstanceOf[RDD[_]]
			val others = otherIds.map(id => SgxMain.rddIds.get(id).asInstanceOf[RDD[_]])
			try {
				Partitioner.defaultPartitioner(rdd, others: _*)
			} catch {
				case e: Exception =>
					logDebug("Exception: " + e.getMessage)
					logDebug(e.getStackTraceString)
					Partitioner.defaultPartitioner(rdd, others: _*)
			}
		}, Duration.Inf)
	}
}
