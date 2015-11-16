package com.dataartisans;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TheUser {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<SimpleEntity> dataStream = env
				.readFile(new AvroInputFormat<>(new Path(args[0]), SimpleEntity.class), args[0])
				.setParallelism(1);


		SingleOutputStreamOperator<SimpleEntity, ?> res = dataStream.flatMap(new FlatMapFunction<SimpleEntity, SimpleEntity>() {
			@Override
			public void flatMap(SimpleEntity simpleEntity, Collector<SimpleEntity> collector) throws Exception {
				collector.collect(simpleEntity);
			}
		});

		res.print();
		env.execute(TheUser.class.getName());
	}
}
