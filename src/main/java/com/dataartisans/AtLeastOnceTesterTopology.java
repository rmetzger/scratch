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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.FileSinkFunctionByMillis;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

public class AtLeastOnceTesterTopology {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setNumberOfExecutionRetries(10);

		if (args.length != 7) {
			System.out.println(" Usage:");
			System.out.println("\tAtLeastOnceTesterTopology <zookeeperHost> <topic> <hdfsWritePath>" +
					" <killerTopic> <sourceParallelism> <mapParallelism> <sinkParallelism>");
			return;
		}

		// zookeeper host address
		String kafkaHost = args[0];
		String topic = args[1];
		String hdfsWritePath = args[2];
		String killerTopic = args[3];
		int sourceParallelism = Integer.parseInt(args[4]);
		int mapParallelism = Integer.parseInt(args[5]);
		int sinkParallelism = Integer.parseInt(args[6]);

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

		final DataStream result = env.addSource(new PersistentKafkaSource<String>(kafkaHost, topic, new KafkaStringSerializationSchema()))
				.setParallelism(sourceParallelism)

				.map(new MapFunction<String, Tuple2<Integer, Long>>() {
					@Override
					public Tuple2<Integer, Long> map(String line) throws Exception {
						String[] split = line.split(" ");

						int from = Integer.parseInt(split[0].split(":")[1]);
						long element = Long.parseLong(split[1].split(":")[1]);

						System.out.println(from + " " + element);
						return new MyTuple2Writable(from, element);
					}
				})
				.setParallelism(mapParallelism);

		//using HDFS append mode
		JobConf conf = new JobConf();
		conf.set("dfs.support.append", "true");
		HadoopOutputFormat wrapper = new HadoopOutputFormat(new TextOutputFormat<NullWritable,MyTuple2Writable>(), conf);
		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, new Path(hdfsWritePath));

//				.writeAsCsv(hdfsWritePath, FileSystem.WriteMode.OVERWRITE, 10)
		result.addSink(new FileSinkFunctionByMillis<LongWritable>(wrapper, 0L))
				.setParallelism(sinkParallelism);

		env.execute();
	}

	public static class MyTuple2Writable extends Tuple2<Integer, Long> implements Writable {

		MyTuple2Writable(Integer f0, Long f1){
			this.f0 = f0;
			this.f1 = f1;
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			dataOutput.writeInt(f0);
			dataOutput.writeLong(f1);
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			f0 = dataInput.readInt();
			f1 = dataInput.readLong();
		}
	}

}
