package com.dataartisans.eventsession;

import com.dataartisans.utils.ThroughputLogger;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Session window opened and closed based on events
 */
public class EventSessionWindowJob {

    public enum EventType {
        LOGIN,
        LOGOUT,
        AUTH_ACTION, /* Only possible when LOGIN has been done and LOGOUT hasn't been done */
        UNRELATED_ACTION
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.setParallelism(1);
        see.enableCheckpointing(2500);

        DataStream<Event> stream = see.addSource(new EventGenerator(pt)).setParallelism(1);
        stream = stream.flatMap(new StreamShuffler<Event>(15)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.milliseconds(30)) {
            @Override
            public long extractTimestamp(Event element) {
                return element.id;
            }
        });

        stream.keyBy("user").window(new EventSessionWindowAssigner()).apply(new WindowFunction<Event, String, Tuple, EventSessionWindow>() {
            @Override
            public void apply(Tuple tuple, EventSessionWindow window, Iterable<Event> input, Collector<String> out) throws Exception {
                System.out.println("Got new window");
                for(Event e: input) {
                    System.out.println("e = " + e);
                }
            }
        });

    //    stream.print();
        // stream.flatMap(new ThroughputLogger(50_000L)).setParallelism(1);

        see.execute("Test");

    }
}
