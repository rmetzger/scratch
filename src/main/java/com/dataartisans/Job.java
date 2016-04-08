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

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.cassandra.CassandraCommitter;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * CREATE KEYSPACE demo;
 * USE demo;
 * CREATE TABLE events (  time bigint,  userId bigint,  PRIMARY KEY (time));
 *
 *
 * New cassandra:
 * CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
 *
 * on a cluster:
 * ./bin/flink run /home/robert/scratch/target/cassandra-test-1.0-1.0-SNAPSHOT.jar --numKeys 1000 --timeSliceSize 60000 --query "INSERT INTO demo.events (time, userid) VALUES(?,?);" --host cdh544-worker-1.c.astral-sorter-757.internal --keyspace demo --committer-table demo.commits
 */
public class Job {
	private static final Logger LOG = LoggerFactory.getLogger(Job.class);

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000);
		final ParameterTool pt = ParameterTool.fromArgs(args);
		env.setStateBackend(new RocksDBStateBackend(pt.get("rock","file:///home/robert/flink-workdir/cassandra-test-1.0/rocksdb")));


		DataStream<Tuple2<Long, Long>> events = env.addSource(new EventGenerator(pt));
		events.flatMap(new ThroughputLogger<Tuple2<Long, Long>>(32, 100_000L)).setParallelism(1);

		// String host, String insertQuery, CheckpointCommitter committer, TypeSerializer<IN> serializer
	/*	CassandraSink.add(events, pt.getRequired("host"), pt.getRequired("query"),
				new CassandraCommitter(pt.getRequired("host"), pt.getRequired("keyspace"), pt.getRequired("committer-table"))); */

		CassandraSink.addSink(events)
			.setQuery(pt.getRequired("query"))
				//.setClusterBuilder(new ClusterBuilder() {
				//	@Override
				//	protected Cluster buildCluster(Cluster.Builder builder) {
				//		return builder.addContactPoint(pt.getRequired("host")).build();
				//	}
				//})
		//	.setCheckpointCommitter(new CassandraCommitter(pt.getRequired("host"), pt.getRequired("keyspace"), pt.getRequired("committer-table")))
		//	.setIdempotent(true)
			.setHost(pt.getRequired("host"))
			.enableWriteAheadLog()
			.build();



		// execute program
		env.execute("Cassandra Pumper: " +pt.toMap());
	}

	public static class EventGenerator extends RichParallelSourceFunction<Tuple2<Long, Long>> implements Checkpointed<Tuple2<Long,Long>> {
		private final ParameterTool pt;

		//checkpointed
		private Long time = 0L;
		private Long key = 0L;

		private volatile boolean running = true;
		private final long numKeys;
		private final long eventsPerKey;
		//	private final int timeVariance; // the max delay of the events
		private final long timeSliceSize;
		private Random rnd;

		public EventGenerator(ParameterTool pt) {
			this.pt = pt;
			this.numKeys = pt.getLong("numKeys");
			this.eventsPerKey = pt.getLong("eventsPerKeyPerGenerator", 1);
			//this.timeVariance = pt.getInt("timeVariance", 10_000); // 10 seconds
			this.timeSliceSize = pt.getLong("timeSliceSize"); // 1 minute
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
			rnd = new XORShiftRandom(getRuntimeContext().getIndexOfThisSubtask());
			long el = 0;
			while(el < 10_000_000L) {
				while(true) {
					//for (key = 0L; key < numKeys; key++) {
					synchronized (sourceContext.getCheckpointLock()) {
						for (long eventPerKey = 0; eventPerKey < eventsPerKey; eventPerKey++) {
							final Tuple2<Long, Long> out = new Tuple2<>();
							out.f0 = time + rnd.nextInt((int) timeSliceSize); // distribute events within slice size
							out.f1 = key;
							//			 System.out.println("Outputting key " + key + " for time " + time);
							sourceContext.collect(out); el++;
							if (!running) {
								return; // we are done
							}
						}
					}
					if(++key >= numKeys) {
						key = 0L;
						break;
					}
				}
				// advance base time
				time += timeSliceSize;
			}
			Thread.sleep(1_000_000);
			sourceContext.close();
		}

		@Override
		public void cancel() {
			LOG.info("Received cancel in EventGenerator");
			running = false;
		}

		@Override
		public Tuple2<Long, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return new Tuple2<>(this.time, this.key);
		}

		@Override
		public void restoreState(Tuple2<Long, Long> state) throws Exception {
			this.time = state.f0;
			this.key = state.f1;
		}
	}

/*	private static class CassandraSink {
		public static <T extends Tuple> void add(DataStream<T> events, String host, String query, CassandraCommitter cassandraCommitter) {
			events.transform("Cassandra Sink", null,
					new CassandraAtLeastOnceSink<>(host, query, cassandraCommitter, events.getType().createSerializer(events.getExecutionConfig())));
		}
	} */
}
