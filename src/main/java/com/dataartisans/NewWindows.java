package com.dataartisans;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.ProcessingTime;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.omg.CORBA.TIMEOUT;

import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * Created by robert on 10/4/15.
 */
public class NewWindows {

	public static class Type extends Tuple2<Integer, Integer> {

	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().enableTimestamps();

		DataStreamSource<Type> stream = see.addSource(new SourceFunction<Type>() {
			public boolean running = true;

			@Override
			public void run(SourceContext<Type> ctx) throws Exception {
				Random rnd = new Random();
				while (running) {
					Type t = new Type();
					t.f0 = rnd.nextInt();
					t.f1 = 1;
					ctx.collect(t);
					Thread.sleep(1000);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});
		DataStream<Type> window = stream.timeWindowAll(ProcessingTime.of(15, TimeUnit.SECONDS), ProcessingTime.of(5, TimeUnit.SECONDS))
				//.evictor(TimeEvictor.of(ProcessingTime.of(3, TimeUnit.SECONDS)))
		/*  .apply(new AllWindowFunction<Type, Type, TimeWindow>() {
					 @Override
					 public void apply(TimeWindow window, Iterable<Type> values, Collector<Type> out) throws Exception {
						 System.out.println("Window done: "+window);
						 int cnt = 0;
						 Type tf = null;
						 for(Type t: values) {
							 cnt += t.f1;
							 tf = t;
						 }
						 if(tf == null) {
							 System.out.println("Triggered empty window");
							 return;
						 }
						 tf.f1 = cnt;
						 out.collect(tf);

					 }
				 }
		  ); */
		.reduce(new ReduceFunction<Type>() {
			@Override
			public Type reduce(Type type, Type t1) throws Exception {
				System.out.println("Reducing ");
				Type t = new Type();
				t.f0 = type.f0;
				t.f1 = type.f1 + t1.f1;
				return t;
			}
		});
		window.print();

		see.execute("Window me");
	}
}
