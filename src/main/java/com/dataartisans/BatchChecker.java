package com.dataartisans;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.lucene.util.OpenBitSet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BatchChecker {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);

		DataSource<String> files = env.readTextFile("/home/robert/flink-workdir/kafka-datagen/pargen");

		FlatMapOperator<String, Tuple2<Integer, Integer>> els = files.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
			Pattern p = Pattern.compile("received: FROM:([0-9]+) ELEMENT:([0-9]+).*");
			@Override
			public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
				if (value.startsWith("received")) {
				//	return;
					Matcher matches = p.matcher(value);
					if(matches.find()) {
						out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(matches.group(1)), Integer.valueOf(matches.group(2))));
					}
				}
			}
		});

	/*	els.sortPartition(1, Order.ANY).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, String>() {

			@Override
			public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<String> collector) throws Exception {
				OpenBitSet checker = new OpenBitSet();

				for (Tuple2<Integer, Integer> fromAndElement : iterable) {
					System.out.println("Got element " + fromAndElement);
					if (checker.get(fromAndElement.f1)) {
						System.out.println("Element " + fromAndElement + " seen twice");
					}
					checker.set(fromAndElement.f1);
				}

				long max = checker.prevSetBit(checker.length());

				checker.flip(0, max);

				long firstNotProcessed = checker.nextSetBit(0);


				if (firstNotProcessed != max) {
					collector.collect("Test PASSED");
				} else {
					collector.collect("Test FAILED");
				}
				collector.collect(firstNotProcessed + " is the first num not processed (out of: " + max + ")");
				collector.collect("----");
			}
		}).writeAsText("/home/robert/flink-workdir/kafka-datagen/debugging/result"); */

		els.groupBy(0).sortGroup(1, Order.ANY).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, String>() {

			@Override
			public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<String> collector) throws Exception {
				OpenBitSet checker = new OpenBitSet();

				for (Tuple2<Integer, Integer> fromAndElement : iterable) {
					System.out.println("Got element " + fromAndElement);
					if (checker.get(fromAndElement.f1)) {
						System.out.println("Element " + fromAndElement + " seen twice");
					}
					checker.set(fromAndElement.f1);
				}

				long max = checker.prevSetBit(checker.length());

				checker.flip(0, max);

				long firstNotProcessed = checker.nextSetBit(0);


				if (firstNotProcessed != max) {
					collector.collect("Test PASSED");
				} else {
					collector.collect("Test FAILED");
				}
				collector.collect(firstNotProcessed + " is the first num not processed (out of: " + max + ")");
				collector.collect("----");
			}
		}).writeAsText("/home/robert/flink-workdir/kafka-datagen/debugging/result", FileSystem.WriteMode.OVERWRITE);

		env.execute("batch checker");

	}
}
