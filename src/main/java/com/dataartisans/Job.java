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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

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

class XORShiftRandom extends Random {
  private long seed = System.nanoTime();

  public XORShiftRandom() {
  }
  protected int next(int nbits) {
    // N.B. Not thread-safe!
    long x = this.seed;
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    this.seed = x;
    x &= ((1L << nbits) -1);
    return (int) x;
  }
}


public class Job {
	
	private static String[] requestType = {"GET", "POST", "PUT", "DELETE"};
	
	private static String[] realWordsDict;
	private static String[] userAgents;
	
	static {
		BufferedReader br;
		// get real words
		try {
			URL rsrc = Job.class.getResource("/dictionary.txt");
			System.err.println("rs="+rsrc);
			br = new BufferedReader( new InputStreamReader(rsrc.openStream()));
			String line;
			List<String> el = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
			   el.add(line.trim());
			}
			br.close();
			realWordsDict = el.toArray(new String[el.size()]);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// get user agents
		try {
			URL rsrc = Job.class.getResource("/ua.txt");
			System.err.println("rs="+rsrc);
			br = new BufferedReader( new InputStreamReader(rsrc.openStream()));
			String line;
			List<String> el = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
			   el.add(line.trim());
			}
			br.close();
			userAgents = el.toArray(new String[el.size()]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getRandomRealWord(Random rnd) {
		return realWordsDict[rnd.nextInt(realWordsDict.length-1)];
	}
	
	public static String getRandomUA(Random rnd) {
		return userAgents[rnd.nextInt(userAgents.length-1)];
	}
	
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = /*new CollectionEnvironment(); */ ExecutionEnvironment.getExecutionEnvironment();
		
		int dop = Integer.valueOf(args[0]);
		String outPath = args[1];
		long finalSizeGB = Integer.valueOf(args[2]);
		final long bytesPerMapper = ((finalSizeGB * 1024 * 1024 * 1024) / dop);
		System.err.println("Generating Log data with the following properties:\n"
				+ "dop="+dop+" outPath="+outPath+" finalSizeGB="+finalSizeGB+" bytesPerMapper="+bytesPerMapper);
		
		DataSet<String> empty = env.fromCollection(Arrays.asList(new String[] {""}));
		DataSet<String> logLine = empty.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				Random rnd = new XORShiftRandom();
				StringBuffer sb = new StringBuffer();
				long bytesGenerated = 0;
				while(true) {
					// write ip:
					sb.append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255));
					sb.append(" - - ["); // some spaces
					sb.append( (new Date(Math.abs(rnd.nextLong())).toString()));
					sb.append("] \"");
					sb.append(requestType[rnd.nextInt(requestType.length-1)]);
					sb.append(' ');
					if(rnd.nextBoolean()) {
						// access to album
						sb.append("/album.php?picture=").append(rnd.nextInt());
					} else {
						// access search
						sb.append("/search.php?term=");
						int terms = rnd.nextInt(8);
						for(int i = 0; i < terms; i++) {
							sb.append(getRandomRealWord(rnd)).append('+');
						}
					}
					sb.append(" HTTP/1.1\" ").append(getRandomUA(rnd));
					if(sb.charAt(sb.length()-1) != '\n') {
						sb.append('\n');
					}
					final String str = sb.toString();
					bytesGenerated += str.length();
					out.collect(str);
					if(bytesGenerated > bytesPerMapper) {
						break;
					}
				}
			}
		}).setParallelism(dop);
		logLine.writeAsText(outPath, WriteMode.OVERWRITE);
		env.execute("Flink Distributed Log Data Generator");
	}
}
