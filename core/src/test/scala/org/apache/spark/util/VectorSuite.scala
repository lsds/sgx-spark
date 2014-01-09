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

package org.apache.spark.util

import scala.util.Random

import org.scalatest.FunSuite

/**
 * Tests org.apache.spark.util.Vector functionality
 */
class VectorSuite extends FunSuite {

  def verifyVector(vector: Vector, expectedLength: Int) = {
    assert(vector.length == expectedLength); // Array must be of expected length
    assert(vector.length == vector.elements.distinct.length); // Values should not repeat
    assert(vector.sum > 0); // All values must not be 0
    assert(vector.sum < vector.length); // All values must not be 1
    assert(vector.elements.product > 0); // No value is 0
  }

  test("random with default random number generator") {
    val vector100 = Vector.random(100);
    verifyVector(vector100, 100);
  }

  test("random with given random number generator") {
    val vector100 = Vector.random(100, new Random(100));
    verifyVector(vector100, 100);
  }
}
