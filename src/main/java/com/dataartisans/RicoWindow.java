package com.dataartisans;

import com.google.common.collect.Ordering;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RicoWindow {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
		see.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);

		DataStream<String> source = see.addSource(new SourceFunction<String>() {
			public boolean running = true;


			@Override
			public void run(SourceContext<String> sourceContext) throws Exception {
				long i = 0;
				while (running) {
					if (i++ % 50 == 0) {
						Thread.sleep(1);
					}
					sourceContext.collect(RandomStringUtils.randomAlphabetic(4));
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		DataStream<List<String>> result = source.window(Time.of(1, TimeUnit.SECONDS)).mapWindow(new WindowMapFunction<String, List<String>>() {

			@Override
			public void mapWindow(Iterable<String> iterable, Collector<List<String>> collector) throws Exception {
				List<String> top5 = Ordering.natural().greatestOf(iterable, 5);
				collector.collect(top5);
			}
		}).flatten();

		result.print();

		see.execute("Top 5 with 1000 elements/second");
	}
}
