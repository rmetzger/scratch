package com.dataartisans;

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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.util.JavaDefaultStringSchema;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;

/**
 * This example shows an implementation of WordCount with data from a text
 * socket. To run the example make sure that the service providing the text data
 * is already up and running.
 * 
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 * 
 * 
 * <p>
 * Usage:
 * <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt; &lt;result path&gt;</code>
 * <br>
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use StreamExecutionEnvironment.socketTextStream
 * <li>write a simple Flink program,
 * <li>write and use user-defined functions.
 * </ul>
 * 
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
public class SocketTextStreamWordCount {

	private static String[] requestType = {"GET", "POST", "PUT", "DELETE"};

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {


		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		DataStreamSource<Long> seq = env.generateSequence(0, Integer.parseInt(args[0]));
		seq.flatMap(new FlatMapFunction<Long, String>() {
			@Override
			public void flatMap(Long value, Collector<String> out) throws Exception {
				Random rnd = new Utils.XORShiftRandom();
				StringBuffer sb = new StringBuffer();
				long element = 0;
				while(true) {
					// write ip:
					sb.append("FROM:");
					sb.append(value);
					sb.append("ELEMENT:");
					sb.append(element++);
					// write ip:
					sb.append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255));
					sb.append(" - - ["); // some spaces
					sb.append( (new Date(Math.abs(rnd.nextLong())).toString()));
					sb.append("] \"");
					sb.append(requestType[rnd.nextInt(requestType.length-1)]);
					sb.append(' ');
					if(rnd.nextBoolean()) {
						// access to album
						sb.append("/album.php?picture=").append(rnd.nextInt());
					} else {
						// access search
						sb.append("/search.php?term=");
						int terms = rnd.nextInt(8);
						for(int i = 0; i < terms; i++) {
							sb.append(Utils.getRandomRealWord(rnd)).append('+');
						}
					}
					sb.append(" HTTP/1.1\" ").append(Utils.getRandomUA(rnd));
					/*if(sb.charAt(sb.length()-1) != '\n') {
						sb.append('\n');
					} */
					final String str = sb.toString();
					sb.delete(0, sb.length());
					out.collect(str);
				}
			}
		}).addSink(new KafkaSink<String>(args[1], args[2], new JavaDefaultStringSchema()));



		// execute program
		env.execute("Spill some data into Kafka");
	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}	
}
