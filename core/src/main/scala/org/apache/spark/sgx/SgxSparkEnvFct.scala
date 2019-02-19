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
import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.ThreadUtils

object SgxSparkEnvFct {

  private var serializer: Serializer = null
  private var conf: SparkConf = null;
  private var outputCommitCoordinator: OutputCommitCoordinator = null
  
  def getSerializer = {
    if (serializer == null) serializer = GetSerializer().send()
    serializer
  }

  def getConf = {
    if (conf == null) conf = GetConf().send()
    conf
  }

  def getOutputCommitCoordinator = {
    if (outputCommitCoordinator == null) outputCommitCoordinator = GetOutputCommitCoordinator().send()
    outputCommitCoordinator
  }
}

private case class GetSerializer() extends SgxMessage[Serializer] {
  def execute() = ThreadUtils.awaitResult( Future { SparkEnv.get.serializer }, Duration.Inf)
}

private case class GetConf() extends SgxMessage[SparkConf] {
  def execute() = ThreadUtils.awaitResult( Future { SparkEnv.get.conf }, Duration.Inf)
}

private case class GetOutputCommitCoordinator() extends SgxMessage[OutputCommitCoordinator] {
  def execute() = ThreadUtils.awaitResult( Future { SparkEnv.get.outputCommitCoordinator }, Duration.Inf)
}