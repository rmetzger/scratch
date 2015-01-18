package flink.generators;

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

import io.airlift.tpch.Part;
import io.airlift.tpch.PartGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.SplittableIterator;

import java.util.Iterator;


public class Job {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DistributedTPCH gen = new DistributedTPCH(env);
		gen.setScale(1.0);

		DataSet<Part> parts = gen.generateParts();

		// execute program
		env.execute("Flink Java API Skeleton");
	}


	private static class DistributedTPCH {

		private double scale;
		private ExecutionEnvironment env;


		public DistributedTPCH(ExecutionEnvironment env) {
			this.env = env;
		}

		public void setScale(double scale) {
			this.scale = scale;
		}

		public double getScale() {
			return scale;
		}

		public DataSet<Part> generateParts() {

			SplittableIterator<Part> si = new PartSplittableIterator(scale, env.getDegreeOfParallelism());
			env.fromParallelCollection(si, Part.class);
			return null;
		}
	}
	public static class PartSplittableIterator implements SplittableIterator<Part> {
		private double scale;
		private int degreeOfParallelism;

		public PartSplittableIterator(double scale, int degreeOfParallelism) {
			this.scale = scale;
			this.degreeOfParallelism = degreeOfParallelism;
			// PartGenerator pg = new PartGenerator(scale)
		}

		@Override
		public Iterator<Part>[] split(int numPartitions) {

			return new Iterator<Part>[0];
		}

		@Override
		public Iterator<Part> getSplit(int num, int numPartitions) {
			return null;
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return this.degreeOfParallelism;
		}

		//------------------------ Iterator -----------------------------------
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Part next() {
			return null;
		}
	}
}
