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

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.flink.stormcompatibility.api.FlinkLocalCluster;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.GlobalPartitionInformation;

import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

	public static void main(String[] args) throws Exception {

		// test
		ClassLoader cl = Job.class.getClassLoader();
		InputStream resourceStream = cl.getResourceAsStream("web/index.html");
		System.out.println("Rstr = " + resourceStream );
		Enumeration<URL> en = cl.getResources("web/index.html");
		if (en.hasMoreElements()) {
			URL url = en.nextElement();
			System.out.println("url = "+url);
			JarURLConnection urlcon = (JarURLConnection) (url.openConnection());
			try (JarFile jar = urlcon.getJarFile();) {
				Enumeration<JarEntry> entries = jar.entries();
				while (entries.hasMoreElements()) {
					String entry = entries.nextElement().getName();
					System.out.println(entry);
				}
			}
		}
		System.out.println("Cl instance "+cl.getClass().getName());
		System.exit(1);

		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final FlinkTopologyBuilder builder = new FlinkTopologyBuilder();


		Broker broker = new Broker("localhost", 9092);
		GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
		partitionInfo.addPartition(0, broker);
		StaticHosts hosts = new StaticHosts(partitionInfo);

		SpoutConfig spoutConfig = new SpoutConfig(hosts, "stuff", "/", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		builder.setSpout("kafkaSpout", kafkaSpout, 1);

		builder.setBolt("printBolt", new PrintBolt()).shuffleGrouping("kafkaSpout");

		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology("Kafka test", null, builder.createTopology());

		Thread.sleep(100000);
	}



	private static class PrintBolt implements IBasicBolt {
		@Override
		public void prepare(Map stormConf, TopologyContext context) {

		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			System.out.println("Got input "+input);
		}

		@Override
		public void cleanup() {

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}
}
