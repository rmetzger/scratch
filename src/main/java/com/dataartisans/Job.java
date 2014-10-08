package com.dataartisans;

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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

	private static final Logger LOG = LoggerFactory.getLogger(Job.class);


	public static final int WINDOW_SIZE = 3;
	public static final int WORKLOAD_MB = 8;

	public static class MyCrazyWorkload implements Value {
		private byte[] myData;
		public MyCrazyWorkload() {
			System.out.println("new instance");
			myData = new byte[1024];
		}
		@Override
		public void write(DataOutputView out) throws IOException {
			out.write(myData);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			in.read(myData);
		}
	}

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStream<Tuple2<Integer, MyCrazyWorkload>> input = env.addSource(new SourceFunction<Tuple2<Integer, MyCrazyWorkload>>() {
			@Override
			public void invoke(Collector<Tuple2<Integer, MyCrazyWorkload>> collector) throws Exception {
				Random rnd = new Random(1337); // not so random
				int i=0;
				MyCrazyWorkload wl = new MyCrazyWorkload();
				while(true) {
					collector.collect(new Tuple2(i++, wl)); //Math.abs(rnd.nextInt())
				}
			}
		});

		DataStream<Double> avg = input.window(1000 * WINDOW_SIZE).reduceGroup(new GroupReduceFunction<Tuple2<Integer, MyCrazyWorkload>, Double>() {

			@Override
			public void reduce(Iterable<Tuple2<Integer, MyCrazyWorkload>> values, Collector<Double> out) throws Exception {
				long sum = 0;
				long count = 0;
				for(Tuple2<Integer, MyCrazyWorkload> tup: values) {
					sum += tup.f0;
					count++;
				}
				double d = sum /(double) count;
				out.collect(d);
				LOG.warn("got a = " + d + " sum=" + count + " that is elements/second=" + (count / WINDOW_SIZE));
			}
		});
		avg.print();

		// execute program
		env.execute("Flink Streaming Example");
	}
}
