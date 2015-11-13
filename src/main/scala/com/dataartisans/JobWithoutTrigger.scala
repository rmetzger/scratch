package com.dataartisans

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, DataStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows




object JobWithoutTrigger {
  case class WordCount(word: String, count: Long)

  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pt = ParameterTool.fromArgs(args)
    val text:	DataStream[String]	=	env.readTextFile(pt.getRequired("input"))
    text
      .flatMap (in => in.split(" ") ).map(word => new WordCount(word, 1))
      .keyBy("word")
      .window(GlobalWindows.create())
      .sum("count")
    .writeAsText(pt.getRequired("out"))

    // execute program
    env.execute("Wordcount with stream API")
  }
}
