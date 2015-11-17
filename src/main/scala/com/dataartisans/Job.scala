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

import org.apache.flink.api.common.accumulators.{AccumulatorHelper, LongCounter}
import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
object Job {

  case class Word(w: String, c: Int)

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val pt = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(pt)


    val in = env.readTextFile(pt.getRequired("input"))

    val mapped = in.flatMap { _.split(" ")}.map { new Word(_, 1)}

    /*  val freq = mapped.reduce( (w1, w2) => {
        new Word(w1.w, w1.c + w2.c)
      }) */
    val freq = mapped.reduce(new RichReduceFunction[Word] {
      var longCounter: Option[LongCounter] = Option.empty

      override def open(parameters: Configuration) = {
        val instId = getRuntimeContext.getIndexOfThisSubtask
        longCounter = Option(getRuntimeContext.getLongCounter("reducer-calls-"+instId))
      }
      override def reduce(w1: Word, w2: Word): Word = {
        longCounter.get.add(1L)
        new Word(w1.w, w1.c + w2.c)
      }
    })


    freq.writeAsText(pt.getRequired("out"))

    val result = env.execute("Small word count")


    println("======== ACCUMULATOR RESULTS ================ ")
    println(AccumulatorHelper.getResultsFormated(result.getAllAccumulatorResults))
  }
}
