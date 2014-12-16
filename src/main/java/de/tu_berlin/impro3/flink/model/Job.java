package de.tu_berlin.impro3.flink.model;

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

import de.tu_berlin.impro3.flink.model.tweet.Tweet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

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

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> seq = env.generateSequence(0, 10);
		DataSet<Tweet> tw = seq.flatMap(new FlatMapFunction<Long, Tweet>() {
			@Override
			public void flatMap(Long aLong, Collector<Tweet> coll) throws Exception {
				for(int i = 0; i < 5; i++) {
					Tweet t = new Tweet();
					t.setText("uhlalhla .. thats the text");
					t.setId(aLong);
					coll.collect(t);
				}
			}
		});
		DataSet<Integer> cnts = tw.groupBy("id").reduceGroup(new GroupReduceFunction<Tweet, Integer>() {
			@Override
			public void reduce(Iterable<Tweet> iterable, Collector<Integer> collector) throws Exception {
				int cnt = 0;
				for(Tweet t: iterable) {
					cnt++;
				}
				collector.collect(cnt);
			}
		});
		cnts.print();

		// tw.print();

		// execute program
		env.execute("Flink Java API Skeleton");
	}
}
