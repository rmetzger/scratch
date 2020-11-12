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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import de.robertmetzger.flink.utils.datagenerators.SimpleStringSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 */
public class PreventingTaskManagerShutdown {
	private static final Logger LOG = LoggerFactory.getLogger(PreventingTaskManagerShutdown.class);

	public static void main(String[] args) throws Exception {
		LOG.info("Welcome");
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> strings = env.addSource(new SimpleStringSource(1000));

		strings.map(new StringStringMapFunction()).print();

		env.setParallelism(2);


		env.execute("shutdown prevent0r");
	}

	private static class StringStringMapFunction implements MapFunction<String, String> {
		transient Object lock = new Object();

		@Override
		public String map(String value) throws Exception {
			Thread hook = new Thread(() -> {
				LOG.info("Hook running");
				Object l = new Object();
				synchronized (l) {
					try {
						LOG.info("Hook waiting");
						l.wait();
						LOG.info("notified");
					} catch (InterruptedException e) {
						LOG.info("interrupted");
						throw new RuntimeException("nooo!", e);
					}
				}
			});
			LOG.info("Registered hook");
			Runtime.getRuntime().addShutdownHook(hook);
			Thread.sleep(50000);
			return value;
			/*
			lock = new Object();
			Thread t = new Thread(() -> {
				LOG.info("nondaemon running");
				while (true) {
					try {
						Thread.sleep(1000);
						LOG.info("nondaemon still sleeping");
					} catch (Throwable tr) {
						LOG.info("Ignoring", tr);
					}
				}
			});
			t.setDaemon(false);
			t.start();
			Thread hook = new Thread(() -> {
				LOG.info("Shutdownhook running");
				System.exit(0);
				// trigger JVM hang
				synchronized (lock) {
					LOG.info("sync");
					try {
						LOG.info("waitin ...");
						lock.wait();
						LOG.info("what?");
					} catch (InterruptedException e) {
						LOG.info("aha", e);
					}
				}
				LOG.info("Left lock?");
				System.exit(0);
				/*synchronized (lock) {
					while (true) {
						try {
							lock.wait();
						} catch (InterruptedException e) {
							LOG.info("aha", e);
						}
					}
				} */
			/*});
			hook.setDaemon(false);
			Runtime.getRuntime().addShutdownHook(hook);
			LOG.info("Shutdownhook added");
			while (true) {
				try {
					Thread.sleep(10000);
					LOG.info("map function still sleeping");
				} catch (Throwable tr) {
					LOG.debug("Ignoring", tr);
				}
			} */
			// return value;
		}
	}
}
