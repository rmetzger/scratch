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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.lucene.util.OpenBitSet;

public class CheckingTopology {

	public static void main(String[] args) {

		String hdfsFileLocation = args[0];

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setDegreeOfParallelism(1);

		DataSource<Tuple2<Integer, Long>> tuples =
				env.readCsvFile(hdfsFileLocation).fieldDelimiter(",").types(Integer.class, Long.class);

		tuples.groupBy(0)
				.reduceGroup(new GroupReduceFunction<Tuple2<Integer,Long>, Boolean>() {

					@Override
					public void reduce(Iterable<Tuple2<Integer, Long>> iterable, Collector<Boolean> collector) throws Exception {
						OpenBitSet checker = new OpenBitSet();

						for (Tuple2<Integer, Long> fromAndElement : iterable) {
							checker.set(fromAndElement.f1);
						}

						long max = checker.prevSetBit(checker.length());

						checker.flip(0, max);

						long firstNotProcessed = checker.nextSetBit(0);

						System.out.println(firstNotProcessed + " is the first num not processed (out of: " + max + ")");

						if (firstNotProcessed != max) {
							collector.collect(false);
						} else {
							collector.collect(true);
						}
					}
				})
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
