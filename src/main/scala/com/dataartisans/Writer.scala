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
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

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
object Writer {
  def main(args: Array[String]) {
    val para = ParameterTool.fromArgs(args)
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new SourceFunction[String] {
      var running = true

      override def cancel(): Unit = {
        running = false
      }

      override def run(ctx: SourceContext[String]): Unit = {
        var k = 0L
        while(running) {
          ctx.collect("Some string " + k)
          k += 1
          Thread.sleep(200L)
        }
      }
    })
    env.setParallelism(1)

    stream.addSink(new FlinkKafkaProducer08[String](para.getRequired("topic"), new SimpleStringSchema, para.getProperties))

    stream.addSink(new FlinkKafkaProducer08[String](para.getRequired("topic"), new SimpleStringSchema, para.getProperties))


    // execute program
    env.execute("Write into Kafka")
  }
}
