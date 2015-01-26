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

package flink.generators.programs;

import flink.generators.core.DistributedTPCH;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * TPC H Schema Diagramm: https://www.student.cs.uwaterloo.ca/~cs348/F14/TPC-H_Schema.png
 */
public class TPCHGeneratorExample {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DistributedTPCH gen = new DistributedTPCH(env);
		gen.setScale(100.0);

		DataSet<Order> orders = gen.generateOrders();

		DataSet<LineItem> lineitem = gen.generateLineItems();

		DataSet<Tuple2<Order, LineItem>> oxl = orders.join(lineitem).where(new KeySelector<Order, Long>() {
			@Override
			public Long getKey(Order value) throws Exception {
				return value.getOrderKey();
			}
		}).equalTo(new KeySelector<LineItem, Long>() {
			@Override
			public Long getKey(LineItem value) throws Exception {
				return value.getOrderKey();
			}
		});
		oxl.print();

		// execute program
		env.execute("Flink Java API Skeleton");
	}

}
