package flink.generators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.junit.Test;

import java.lang.reflect.Method;

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

public class TPCHGeneratorExampleTest {

	@Test
	public void testGeneratorInstantiation() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DistributedTPCH gen = new DistributedTPCH(env);
		gen.setScale(0.1);
		String[] methods = new String[] {"generateParts", "generateLineItems"};
		for(String method : methods) {
			Method rMethod = gen.getClass().getMethod(method);
			DataSet ds = (DataSet) rMethod.invoke(gen);
			ds.output(new DiscardingOutputFormat());
		}
		// execute program
		env.execute("testGeneratorInstantiation");
	}
}
