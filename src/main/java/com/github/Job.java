package com.github;

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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.io.DiscardingOuputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import java.io.IOException;
import java.util.Arrays;


public class Job {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<String> strings = env.fromCollection(Arrays.asList(new String[]{"1-test", "2-is", "1-haha", "2-well"}));

		JobConf jc = new JobConf();
		MultipleTextOutputFormat<NullWritable, Text> multipleTextOutputFormat = new MultipleTextOutputFormat<NullWritable, Text>();
		FileOutputFormat.setOutputPath(jc, new Path("test/"));
		MultipleOutputs.addNamedOutput(jc, "out1", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(jc, "out2", TextOutputFormat.class, NullWritable.class, Text.class);
		HadoopOutputFormat<NullWritable, Text> hdout = new HadoopOutputFormat<NullWritable, Text>(multipleTextOutputFormat, jc);
		Configuration flinkConf = new Configuration();
		flinkConf.setBytes("HDjobConf", HadoopConfigurationSerializationUtility.configurationToBytes(jc));

		strings.flatMap(new DummyWrapper()).withParameters(flinkConf).output(hdout);

		// execute program
		env.execute("Flink MultipleOutputs example");
	}

	public static class DummyWrapper extends RichFlatMapFunction<String, Tuple2<NullWritable, Text>> {
		private OutputCollector out1;
		private OutputCollector out2;
		private MultipleOutputs multipleOutputs;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			byte[] b = parameters.getBytes("HDjobConf", null);
			if (b == null) {
				throw new RuntimeException("The configuration has not been set properly");
			}
			JobConf jobConf = (JobConf) HadoopConfigurationSerializationUtility.bytesToConfiguration(b);
			int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
			TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_"
					+ String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s"," ").replace(" ", "0")
					+ Integer.toString(taskNumber + 1)
					+ "_0");

			jobConf.set("mapred.task.id", taskAttemptID.toString());
			jobConf.setInt("mapred.task.partition", taskNumber + 1);
			// for hadoop 2.2
			jobConf.set("mapreduce.task.attempt.id", taskAttemptID.toString());
			jobConf.setInt("mapreduce.task.partition", taskNumber + 1);


			FileOutputCommitter fileOutputCommitter = new FileOutputCommitter();

			JobContext jobContext;
			try {
				jobContext = HadoopUtils.instantiateJobContext(jobConf, new JobID());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			fileOutputCommitter.setupJob(jobContext);

			multipleOutputs = new MultipleOutputs(jobConf);
			out1 = multipleOutputs.getCollector("out1", new HadoopDummyReporter());
			out2 = multipleOutputs.getCollector("out2", new HadoopDummyReporter());
		}

		@Override
		public void flatMap(String s, Collector<Tuple2<NullWritable, Text>> out) throws Exception {
			if (s.startsWith("1-")) {
				out1.collect(NullWritable.get(), new Text(s));
			}
			if (s.startsWith("2-")) {
				out2.collect(NullWritable.get(), new Text(s));
			}
			// out.collect(new Tuple2<NullWritable, Text>(NullWritable.get(), new Text(s)));
		}

		@Override
		public void close() throws Exception {
			super.close();
			multipleOutputs.close();
		}
	}

	public static class HadoopConfigurationSerializationUtility {

		public static byte[] configurationToBytes(org.apache.hadoop.conf.Configuration conf) throws IOException {
			DataOutputBuffer bos = new DataOutputBuffer();
			bos.writeUTF(conf.getClass().getCanonicalName());
			conf.write(bos);
			return bos.getData();
		}

		public static org.apache.hadoop.conf.Configuration bytesToConfiguration(byte[] b) throws IOException, ClassNotFoundException {
			DataInputBuffer din = new DataInputBuffer();
			din.reset(b, b.length);
			String className = din.readUTF();
			Class<org.apache.hadoop.conf.Configuration> clazz =  (Class<org.apache.hadoop.conf.Configuration>) Class.forName(className);
			org.apache.hadoop.conf.Configuration conf = InstantiationUtil.instantiate(clazz);
			conf.readFields(din);
			return conf;
		}
	}
}
