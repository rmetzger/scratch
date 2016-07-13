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

import java.util.Collections

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaGroupConsumer
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchemaWrapper, SimpleStringSchema}

object Reader {
  def main(args: Array[String]) {
    val para = ParameterTool.fromArgs(args)
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    env.setNumberOfExecutionRetries(4)
    env.enableCheckpointing(15000)


    val messageStream = env.addSource(new FlinkKafkaGroupConsumer[String](
      Collections.singletonList(para.getRequired("topic")), new KeyedDeserializationSchemaWrapper(new SimpleStringSchema()), para.getProperties))

    messageStream.print()

    env.execute("Read from Kafka example")
  }
}
