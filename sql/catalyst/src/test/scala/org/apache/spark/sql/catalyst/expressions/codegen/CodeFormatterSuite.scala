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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util._


class CodeFormatterSuite extends SparkFunSuite {

  def testCase(name: String)(input: String)(expected: String): Unit = {
    test(name) {
      val sourceCode = new CodeAndComment(input, Map.empty)
      if (CodeFormatter.format(sourceCode).trim !== expected.trim) {
        fail(
          s"""
             |== FAIL: Formatted code doesn't match ===
             |${sideBySide(CodeFormatter.format(sourceCode).trim, expected.trim).mkString("\n")}
           """.stripMargin)
      }
    }
  }

  test("removing overlapping comments") {
    val code = new CodeAndComment(
      """/*project_c4*/
        |/*project_c3*/
        |/*project_c2*/
      """.stripMargin,
      Map(
        "/*project_c4*/" -> "// (((input[0, bigint, false] + 1) + 2) + 3))",
        "/*project_c3*/" -> "// ((input[0, bigint, false] + 1) + 2)",
        "/*project_c2*/" -> "// (input[0, bigint, false] + 1)"
      ))

    val reducedCode = CodeFormatter.stripOverlappingComments(code)
    assert(reducedCode.body === "/*project_c4*/")
  }

  testCase("basic example") {
    """class A {
      |blahblah;
      |}""".stripMargin
  }{
    """
      |/* 001 */ class A {
      |/* 002 */   blahblah;
      |/* 003 */ }
    """.stripMargin
  }

  testCase("nested example") {
    """class A {
      | if (c) {
      |duh;
      |}
      |}""".stripMargin
  } {
    """
      |/* 001 */ class A {
      |/* 002 */   if (c) {
      |/* 003 */     duh;
      |/* 004 */   }
      |/* 005 */ }
    """.stripMargin
  }

  testCase("single line") {
    """class A {
      | if (c) {duh;}
      |}""".stripMargin
  }{
    """
      |/* 001 */ class A {
      |/* 002 */   if (c) {duh;}
      |/* 003 */ }
    """.stripMargin
  }

  testCase("if else on the same line") {
    """class A {
      | if (c) {duh;} else {boo;}
      |}""".stripMargin
  }{
    """
      |/* 001 */ class A {
      |/* 002 */   if (c) {duh;} else {boo;}
      |/* 003 */ }
    """.stripMargin
  }

  testCase("function calls") {
    """foo(
      |a,
      |b,
      |c)""".stripMargin
  }{
    """
      |/* 001 */ foo(
      |/* 002 */   a,
      |/* 003 */   b,
      |/* 004 */   c)
    """.stripMargin
  }

  testCase("single line comments") {
    """// This is a comment about class A { { { ( (
      |class A {
      |class body;
      |}""".stripMargin
  }{
    """
      |/* 001 */ // This is a comment about class A { { { ( (
      |/* 002 */ class A {
      |/* 003 */   class body;
      |/* 004 */ }
    """.stripMargin
  }

  testCase("single line comments /* */ ") {
    """/** This is a comment about class A { { { ( ( */
      |class A {
      |class body;
      |}""".stripMargin
  }{
    """
      |/* 001 */ /** This is a comment about class A { { { ( ( */
      |/* 002 */ class A {
      |/* 003 */   class body;
      |/* 004 */ }
    """.stripMargin
  }

  testCase("multi-line comments") {
    """  /* This is a comment about
      |class A {
      |class body; ...*/
      |class A {
      |class body;
      |}""".stripMargin
  }{
    """
      |/* 001 */ /* This is a comment about
      |/* 002 */ class A {
      |/* 003 */   class body; ...*/
      |/* 004 */ class A {
      |/* 005 */   class body;
      |/* 006 */ }
    """.stripMargin
  }

  // scalastyle:off whitespace.end.of.line
  testCase("reduce empty lines") {
    CodeFormatter.stripExtraNewLines(
      """class A {
        |
        |
        | /*** comment1 */
        |
        | class body;
        |
        |
        | if (c) {duh;}
        | else {boo;}
        |}""".stripMargin)
  }{
    """
      |/* 001 */ class A {
      |/* 002 */   /*** comment1 */
      |/* 003 */   class body;
      |/* 004 */ 
      |/* 005 */   if (c) {duh;}
      |/* 006 */   else {boo;}
      |/* 007 */ }
    """.stripMargin
  }
  // scalastyle:on whitespace.end.of.line
}
