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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.robertmetzger.flink.utils.datagenerators.SimpleStringSource;
import de.robertmetzger.flink.utils.performance.ThroughputLogger;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("file:///tmp/test"));
		System.out.println("res = " + fs.delete(new Path("file:///tmp/test"), false));
		System.exit(0);

		File parent = new File("/tmp");
		File f = new File(parent, "../etc/passwd");
		System.out.println("f = " + f.exists() +" f path " + f.getAbsolutePath());

		System.exit(0);
		LOG.info("Welcome");
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Schema schema = new Schema.Parser().parse("{\n" +
				"   \"type\" : \"record\",\n" +
				"   \"namespace\" : \"Tutorialspoint\",\n" +
				"   \"name\" : \"Employee\",\n" +
				"   \"fields\" : [\n" +
				"      { \"name\" : \"Name\" , \"type\" : \"string\" },\n" +
				"      { \"name\" : \"Age\" , \"type\" : \"int\" }\n" +
				"   ]\n" +
				"}");
		DataStream<String> strings = env.addSource(new SimpleStringSource(1000));
		DataStream<GenericRecord> records = strings.map(inputStr -> {
			GenericData.Record rec = new GenericData.Record(schema);
			rec.put(0, inputStr);
			return rec;
		});

		env.setParallelism(2);

		strings.flatMap(new CountToAccumulator<>()).setParallelism(1);
		strings.print();


		JobClient client = env.executeAsync("Test");
		Thread.sleep(5000);

		String svpLoc = client.triggerSavepoint("file:///Users/robert/Projects/flink-workdir/flink-1.12-test/svp/").get();
		LOG.info("loc  " + svpLoc);
		Thread.sleep(1000);

		CompletableFuture<JobStatus> status = client.getJobStatus();
		LOG.info("status = " + status.get());

	/*	CompletableFuture<Map<String, Object>> accumulators = client.getAccumulators(StreamingJob.class.getClassLoader());
		LOG.info("yo = " + accumulators);
		LOG.info("accus = " + accumulators.get(5, TimeUnit.SECONDS)); */

	/*	new Thread(() -> {
			try {
				LOG.info("Requesting result");
				LOG.info("result = " + client.getJobExecutionResult(StreamingJob.class.getClassLoader()).get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}).start();
		Thread.sleep(1000);
		client.cancel();
		LOG.info("cancel"); */

		/*accumulators = client.getAccumulators(StreamingJob.class.getClassLoader());
		LOG.info("yo = " + accumulators);
		LOG.info("accus = " + accumulators.get(5, TimeUnit.SECONDS)); */
		/*for(int i = 0; i < 20; i++) {
			Thread.sleep(1000);
			LOG.info("get status");



		} */
		Thread.sleep(5000);
	}

	private static class CountToAccumulator<T> extends RichFlatMapFunction<T, T> {
		private final LongCounter recordCounter = new LongCounter();
		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator("records", recordCounter);
		}

		@Override
		public void flatMap(T t, Collector<T> collector) throws Exception {
			recordCounter.add(1L);
		}
	}
}
