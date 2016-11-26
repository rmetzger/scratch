//package com.dataartisans;
//
///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import org.apache.activemq.ActiveMQConnectionFactory;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.activemq.AMQSink;
//import org.apache.flink.streaming.connectors.activemq.AMQSinkConfig;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//
//
///**
// * Skeleton for a Flink Streaming Job.
// *
// * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
// * file in the same package/directory or have a look at the website.
// *
// * You can also generate a .jar file that you can submit on your Flink
// * cluster.
// * Just type
// * 		mvn clean package
// * in the projects root directory.
// * You will find the jar in
// * 		target/quickstart-1.2-tests-1.0-SNAPSHOT.jar
// * From the CLI you can then run
// * 		./bin/flink run -c com.dataartisans.StreamingJob target/quickstart-1.2-tests-1.0-SNAPSHOT.jar
// *
// * For more information on the CLI see:
// *
// * http://flink.apache.org/docs/latest/apis/cli.html
// */
//public class Producer {
//
//	public static void main(String[] args) throws Exception {
//		// set up the streaming execution environment
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		DataStream<String> src = env.addSource(new SourceFunction<String>() {
//			boolean running = true;
//
//			@Override
//			public void run(SourceContext<String> sourceContext) throws Exception {
//
//				while (running) {
//					sourceContext.collect("abc");
//					// Thread.sleep(5);
//				}
//			}
//
//			@Override
//			public void cancel() {
//				running = false;
//			}
//		});
//
//		AMQSinkConfig.AMQSinkConfigBuilder<String> sinkConf = new AMQSinkConfig.AMQSinkConfigBuilder<>();
//		sinkConf.setConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:61616?trace=false&soTimeout=60000"));
//		sinkConf.setDestinationName("test");
//		sinkConf.setSerializationSchema(new SimpleStringSchema());
//		src.addSink(new AMQSink<>(sinkConf.build()));
//
//		// execute program
//		env.execute("Flink Streaming Java API Skeleton");
//	}
//}
