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

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.broadcast.SgxBroadcastProviderIdentifier

class SgxMainRunner(com: SgxCommunicator) extends SgxCallable[Unit] with Logging {
	def call(): Unit = {
		while(isRunning) {
			val recv = com.recvOne()
			logDebug("Received: " + recv)

			val result = recv match {
				case x: SgxMessage[_] => x.execute()

				case x: SgxBroadcastProviderIdentifier =>
					x.connect()
					true
			}
			
			logDebug("Result: " + result + " (" + result.getClass().getSimpleName + ")")
			if (result != null)
				com.sendOne(result)
		}

		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}