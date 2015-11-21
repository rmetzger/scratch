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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import java.util.Random;

public class Job {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool parameters = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parameters);

		String path = parameters.get("path");
		DataSet<String> data = env.readTextFile(path);
		for(int i = 0; i < parameters.getInt("numSources"); i++) {
			data = data.union(env.readTextFile(path));
		}
		if(parameters.has("withWorkload")) {
			DataSet<Tuple3<Float, Float, byte[]>> typed = data.map(new MapFunction<String, Tuple3<Float, Float, byte[]>>() {
				final Random rnd = new Random(1337);

				@Override
				public Tuple3<Float, Float, byte[]> map(String s) throws Exception {
					String[] el = s.split(" ");
					return new Tuple3<Float, Float, byte[]>(Float.valueOf(el[0]), Float.valueOf(el[1]), new byte[Math.abs(rnd.nextInt(parameters.getInt("maxbytes")))]);
				}
			});

			DataSet<Tuple3<Float, Float, byte[]>> sums = typed.groupBy(0).sum(1);
			sums.writeAsText(parameters.get("output"), FileSystem.WriteMode.OVERWRITE);
		} else {
			DataSet<Tuple2<Float, Float>> typed = data.map(new MapFunction<String, Tuple2<Float, Float>>() {
				final Random rnd = new Random(1337);

				@Override
				public Tuple2<Float, Float> map(String s) throws Exception {
					String[] el = s.split(" ");
					return new Tuple2<Float, Float>(Float.valueOf(el[0]), Float.valueOf(el[1]));
				}
			});

			DataSet<Tuple2<Float, Float>> sums = typed.groupBy(0).sum(1);
			sums.writeAsText(parameters.get("output"), FileSystem.WriteMode.OVERWRITE);
		}

		// execute program
		env.execute("Simple big union");
	}
}
