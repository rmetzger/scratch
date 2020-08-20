/*
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

package de.robertmetzger;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.robertmetzger.flink.utils.datagenerators.SimpleStringSource;
import de.robertmetzger.flink.utils.performance.ThroughputLogger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * Objectives:
 * - Generate data
 * - use it for a SQL job
 * - group by sql query
 * - attach watermarks using new watermarking api
 */
public class TableApiSql {
	private static final Logger LOG = LoggerFactory.getLogger(TableApiSql.class);

	// public record Simple(int id, String name) { }

	/*public static class Simple {
		public int id;
		public String name;

		public Simple(int id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	public static void main(String[] args) throws Exception {
		LOG.info("welcome");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Simple> strings = env.addSource(new SimpleSource());
		strings.map(s -> {
			// noop
			return s;
		});
		env.execute("yolo");
		// set up the streaming execution environment
	/*	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

		DataStream<Simple> strings = env.addSource(new SimpleSource());

		// ingest a DataStream from an external source

// SQL query with an inlined (unregistered) table
		Table table = bsTableEnv.fromDataStream(strings, $("id"), $("name"));
		Table result = bsTableEnv.sqlQuery(
				"SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

		strings.print();

		env.execute("Test");
		var test = new Simple(1, "Haha"); */
/*
	}

	private static class SimpleSource implements ParallelSourceFunction<Simple> {

		boolean running = true;
		@Override
		public void run(SourceContext<Simple> ctx) throws Exception {
			var test = "String";
			while(running) {
				ctx.collect(new Simple(11, test));
				Thread.sleep(500);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}*/
}
