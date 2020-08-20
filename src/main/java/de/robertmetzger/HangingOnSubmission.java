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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HangingOnSubmission {
	private static final Logger LOG = LoggerFactory.getLogger(HangingOnSubmission.class);

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		LOG.info("Welcome");
		// set up the streaming execution environment
		ExecutionEnvironment batch = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<String> input = batch.createInput(new HangingInputFormat(parameterTool));
		input.printOnTaskManager("yolo");

		batch.execute("this is my test");
	}

	private static class HangingInputFormat implements InputFormat<String, MyInputSplits> {
		private final ParameterTool params;
		private boolean running = true;

		public HangingInputFormat(ParameterTool parameterTool) {
			this.params = parameterTool;
		}

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return null;
		}

		@Override
		public MyInputSplits[] createInputSplits(int minNumSplits) throws IOException {
			if (params.has("failAfterSecs")) {
				int failSecs = params.getInt("failAfterSecs", 30);
				LOG.info("Waiting for {} seconds, then failing", failSecs);
				try {
					Thread.sleep(1000 * failSecs);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
				throw new RuntimeException("Oh my god, I failed");
			}
			int mins = params.getInt("hangMins", 5);
			LOG.info("where am I, Hanging for " + mins, new RuntimeException("Here"));
			try {
				Thread.sleep(1000 * 60 * mins);
			} catch (InterruptedException e) {
				Thread.interrupted();
			}
			return new MyInputSplits[]{new MyInputSplits()};
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(MyInputSplits[] inputSplits) {
			return new InputSplitAssigner() {
				@Override
				public InputSplit getNextInputSplit(String s, int i) {
					return new MyInputSplits();
				}

				@Override
				public void returnInputSplit(List<InputSplit> list, int i) {
					//
				}
			};
		}

		@Override
		public void open(MyInputSplits split) throws IOException {

		}

		@Override
		public boolean reachedEnd() throws IOException {
			return !running;
		}

		@Override
		public String nextRecord(String reuse) throws IOException {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				running = false;
			}
			return "this is all I got";
		}

		@Override
		public void close() throws IOException {

		}
	}

	private static class MyInputSplits implements InputSplit {

		@Override
		public int getSplitNumber() {
			return 0;
		}
	}
}
