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


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {

	private static final Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		JobClient jobClient = executeWith(2, null, pt);

		Thread.sleep(10_000L);
		logger.info("Scaling down job from parallelism 2 --> 1");
		// block until savepoint is created
		String savepointLoc = jobClient.stopWithSavepoint(false, "file:///tmp/flink-svp", SavepointFormatType.CANONICAL).get();
		logger.info("Got savepoint in {}", savepointLoc);

		jobClient = executeWith(1, savepointLoc, pt);
		logger.info("Restored with parallelism 1");
		// waiting for job to finish (which will never happen)
		JobExecutionResult jobExecResult = jobClient.getJobExecutionResult().get();

		logger.info("main() finished. jobExecResult = {}", jobExecResult);
	}

	private static JobClient executeWith(int parallelism, @Nullable String savepointLocation, ParameterTool pt) throws Exception {
		Configuration config = new Configuration();
		if (savepointLocation != null) {
			config.set(SavepointConfigOptions.SAVEPOINT_PATH, savepointLocation);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
		env.enableCheckpointing(5000L);

		env.setParallelism(parallelism);
		env.setMaxParallelism(10);

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers(pt.getRequired("kafka.bootstrap"))
				.setTopics(pt.getRequired("kafka.topics"))
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		dataStream.addSink(new ExampleSinkFunction<>("account-0042-connection-1337-1122334455"));
		dataStream.print();
		return env.executeAsync();
	}

	/**
	 * This is an example Stateful Sink, not using the modern sink2.StatefulSink interfaces of Flink, but the old, raw
	 * interfaces.
	 * More information about the new sink interfaces can be found here:
	 * <a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction">FLIP-191</a>.
	 * The main benefit of the new sink interfaces is support for batch and streaming (unified sink).
	 *
	 * This example sink implementation assumes an underlying system which has a concept of an arbitrary number of channels to write data to.
	 *
	 *
	 */
	private static class ExampleSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction, CheckpointListener {
		private static final Logger logger = LoggerFactory.getLogger(ExampleSinkFunction.class);
		private final String sinkId;
		private ListState<Tuple2<String, Long>> checkpointedState;
		private final List<Tuple2<String, Long>> myChannels = new ArrayList<>();
		private final ExternalApi<T> externalApi = new ExternalApi<>();

		public ExampleSinkFunction(String sinkId) {
			this.sinkId = sinkId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			logger.info("index={}, subtasks={}, maxSubtasks={}", getRuntimeContext().getIndexOfThisSubtask(),
					getRuntimeContext().getNumberOfParallelSubtasks(),
					getRuntimeContext().getMaxNumberOfParallelSubtasks());
		}

		private String getTaskId() {
			return String.format("(%s/%s)", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
		}
		@Override
		public void invoke(T value, Context context) throws Exception {
			logger.info("{} write record = {}, channels = {}", getTaskId(), value, myChannels);
			if (myChannels.size() == 1) {
				myChannels.get(0).f1++;
			} else {
				logger.warn("We are in trouble, somehow");
			}
			externalApi.writeRecord(value);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ListStateDescriptor<Tuple2<String, Long>> descriptor =
					new ListStateDescriptor<>(
							"subscribed-channels",
							TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

			checkpointedState = context.getOperatorStateStore().getListState(descriptor);

			logger.info("{} Initialized state", getTaskId());
			if (context.isRestored()) {
				for (Tuple2<String, Long> element : checkpointedState.get()) {
					myChannels.add(element);
				}
				logger.info("{} Restored channels {}", getTaskId(), myChannels);
			} else {
				// initial start
				String channelId = sinkId + "-" + getRuntimeContext().getIndexOfThisSubtask();
				myChannels.add(Tuple2.of(channelId, 0L));
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			checkpointedState.clear();
			for (Tuple2<String, Long> element : myChannels) {
				checkpointedState.add(element);
			}
			externalApi.flush();
			logger.info("{} Checkpointing state of operator", getTaskId());
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			// in case you need it: this method informs when a checkpoint is completed
			logger.info("{} checkpoint {} complete", getTaskId(), checkpointId);
		}
	}

	private static class ExternalApi<T> implements Serializable {
		void writeRecord(T value) {

		}

		void flush() {

		}
	}
}
