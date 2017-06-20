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

import scala.reflect.ClassTag

import java.io.Serializable

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
class SgxMapPartitionsRDD[U: ClassTag, T: ClassTag] {

//  def sgxCompute(f: (TaskContext, Int, Iterator[T]) => Iterator[U], split: Partition, context: TaskContext): Iterator[U] = {
  def compute(f: (Int, Iterator[T]) => Iterator[U], partIndex: Int, it: Iterator[T]): Iterator[U] = {
	  f(partIndex, it)
  }
}