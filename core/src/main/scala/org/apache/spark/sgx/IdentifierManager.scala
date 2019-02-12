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

import gnu.trove.map.hash.TLongObjectHashMap

import org.apache.spark.internal.Logging

class IdentifierManager[T]() extends Logging {

	private val identifiers = new TLongObjectHashMap[T]()

	def put[U <: T](id: Long, obj: U): U = this.synchronized {
		identifiers.put(id, obj)
		obj
	}

	def get(id: Long): T = this.synchronized {
	  identifiers.get(id)
	}

	def remove[X](id: Long): X = this.synchronized {
		identifiers.remove(id).asInstanceOf[X]
	}
}