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

package org.apache.spark.util.sgx

import java.io.File

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sgx._
import org.apache.spark.util.Utils

import scala.sys.process.Process


class TrustedSGXVM extends SparkFunSuite with SharedSparkContext with Logging {
  private var tempDir: File = _
  @volatile private var _sgxTrustedCommThreadRun: Boolean = true

  def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  override def beforeAll(): Unit = {
    // Enclave side of SHM (trusted)
    setEnv("SGX_ENABLED", "true")
    setEnv("IS_ENCLAVE", "true")
    setEnv("DEBUG_IS_ENCLAVE_REAL", "false")
    setEnv("SGXLKL_SHMEM_FILE", "sgx-lkl-shmem")

    logDebug("Running trusted SgxMain.main()")

    Completor.submit(new Waiter())
    val sgxTrustedCommRunner = new Runnable {
      override def run(): Unit = {
        while (_sgxTrustedCommThreadRun) {
          Completor.submit(new SgxMainRunner(SgxFactory.acceptCommunicator()))
        }
      }
    }
    new Thread(sgxTrustedCommRunner).start()

    super.beforeAll()
    tempDir = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    try {
      _sgxTrustedCommThreadRun = false
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }


  test("SecureWorker operations") {
    logInfo("SecureWorker Starting..")
    while(true) {
      logInfo("SecureWorker Waiting for messages")
      Thread.sleep(1000)
    }
    logInfo("SecureWorker Done")
  }
}

object TrustedSGXVM{
  val launchScript = "/secureVmStart.sh"
  val launchScriptPath = getClass.getResource(launchScript).getPath

//  def main(args: Array[String]): Unit = {
//    val trustedVMproc = Process(Seq("sh", launchScriptPath)).!!
//    println(trustedVMproc)
////    val trustedVMproc = Process("sh", launchScript)
//  }
}

