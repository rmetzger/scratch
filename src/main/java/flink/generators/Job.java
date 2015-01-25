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

import com.google.common.base.Preconditions;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartGenerator;
import io.airlift.tpch.TextPool;
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
		gen.setScale(0.1);

		DataSet<Part> parts = gen.generateParts();
		parts.print();

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
			return env.fromParallelCollection(si, Part.class);
		}
	}

	public static class PartSplittableIterator implements SplittableIterator<Part> {

		private double scale;
		private int degreeOfParallelism;

		public PartSplittableIterator(double scale, int degreeOfParallelism) {
			Preconditions.checkArgument(scale > 0, "Scale must be > 0");
			Preconditions.checkArgument(degreeOfParallelism > 0, "Parallelism must be > 0");

			this.scale = scale;
			this.degreeOfParallelism = degreeOfParallelism;
		}

		@Override
		public Iterator<Part>[] split(int numPartitions) {
			if(numPartitions > this.degreeOfParallelism) {
				throw new IllegalArgumentException("Too many partitions requested");
			}
			Iterator<Part>[] iters = new Iterator[numPartitions];
			for(int i = 1; i <= numPartitions; i++) {
				System.out.println("Creating part ("+i+"/"+numPartitions+")");
				iters[i - 1] = new PartSplittableIterator(i, numPartitions, scale);
			}
			return iters;
		}

		@Override
		public Iterator<Part> getSplit(int num, int numPartitions) {
			if (numPartitions < 1 || num < 0 || num >= numPartitions) {
				throw new IllegalArgumentException();
			}
			return split(numPartitions)[num];
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return this.degreeOfParallelism;
		}

		//------------------------ Iterator -----------------------------------
		private Iterator<Part> iter;
		private static TextPool smallTextPool;
		static {
			smallTextPool = new TextPool(30 * 1024 * 1024, Distributions.getDefaultDistributions()); // 30 MB txt pool
		}

		public PartSplittableIterator(int partNo, int totalParts, double scale) {
			PartGenerator pg = new PartGenerator(scale, partNo, totalParts, Distributions.getDefaultDistributions(), smallTextPool);
			iter = pg.iterator();
		}
		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public Part next() {
			return iter.next();
		}
	}
}
