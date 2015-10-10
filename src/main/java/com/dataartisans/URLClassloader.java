package com.dataartisans;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.net.URL;

public class URLClassloader {

	public static void main(String[] args) throws Exception {

		URL about = URLClassloader.class.getResource("/about.md");

		System.out.println("about = " + about);

		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.addSource(new SourceFunction<Object>() {
			@Override
			public void run(SourceContext<Object> ctx) throws Exception {
				URL about = this.getClass().getResource("/about.md");
				System.out.println("about = "+about);
			}
			@Override
			public void cancel() {}
		});

		see.execute();
	}
}
