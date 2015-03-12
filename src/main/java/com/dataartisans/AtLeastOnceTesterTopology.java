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

package com.dataartisans;

import java.io.IOException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;

public class AtLeastOnceTesterTopology {

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// zookeeper host address
		String kafkaHost = args[0];

		String topic = args[1];
		String hdfsWritePath = args[2];
		String killerTopic = args[3];

		env.addSource(new PersistentKafkaSource<String>(kafkaHost, killerTopic, new KafkaStringSerializationSchema())).addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String s) throws Exception {
				if (s.equals("kill")) {
					throw new Exception("KILL THE JOB!");
				}
			}

			@Override
			public void cancel() {

			}
		}).setParallelism(1);

		env.addSource(new PersistentKafkaSource<String>(kafkaHost, topic, new KafkaStringSerializationSchema())).setParallelism(1)
				.map(new MapFunction<String, Tuple2<Integer, Long>>() {
					@Override
					public Tuple2<Integer, Long> map(String line) throws Exception {
						String[] split = line.split(" ");

						int from = Integer.parseInt(split[0].split(":")[1]);
						long element = Long.parseLong(split[1].split(":")[1]);

						System.out.println(from + " " + element);
						return new Tuple2<Integer, Long>(from, element);
					}
				}).setParallelism(1)

				// TODO use append writing mode
				.writeAsCsv(hdfsWritePath, FileSystem.WriteMode.OVERWRITE, 10).setParallelism(1);

		try {
			env.execute();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
