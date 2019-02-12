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

package org.apache.spark.sgx.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sgx.SgxCommunicator

object SgxBroadcastEnclave {
	private var com: SgxCommunicator = null

	def init(_com: SgxCommunicator): Unit = {
		com = _com
	}

	def destroy[T](bc: Broadcast[T], blocking: Boolean): Unit = {
		com.sendRecv[T](new MsgBroadcastDestroy(bc, blocking))
	}

	def unpersist[T](bc: Broadcast[T], blocking: Boolean): Unit = {
		com.sendRecv[T](new MsgBroadcastUnpersist(bc, blocking))
	}

	def value[T](bc: Broadcast[T]): T = {
		com.sendRecv[T](new MsgBroadcastValue(bc))
	}
}

abstract class MsgBroadcast[R] extends Serializable {
	def apply(): R
}

private case class MsgBroadcastDestroy[T](bc: Broadcast[T], blocking: Boolean) extends MsgBroadcast[Unit] {
	override def apply = bc.destroy(blocking)
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

private case class MsgBroadcastUnpersist[T](bc: Broadcast[T], blocking: Boolean) extends MsgBroadcast[Unit] {
	override def apply = bc.unpersist(blocking)
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ", blocking=" + blocking + ")"
}

private case class MsgBroadcastValue[T](bc: Broadcast[T]) extends MsgBroadcast[T] {
	override def apply = bc.value
	override def toString = this.getClass.getSimpleName + "(bc=" + bc + ")"
}

object MsgBroadcastReqClose extends Serializable {}