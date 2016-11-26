package com.dataartisans.streamutils;

import com.dataartisans.eventsession.Event;
import com.dataartisans.eventsession.EventGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Session window opened and closed based on events
 */
public class PercentileWatermarkJob {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.setParallelism(1);

        DataStream<Event> stream = see.addSource(new EventGenerator(pt)).setParallelism(1);
      //  stream = stream.flatMap(new StreamShuffler<Event>(15)).assignTimestampsAndWatermarks(new PercentileWatermarkGenerator());



    //    stream.print();
        // stream.flatMap(new ThroughputLogger(50_000L)).setParallelism(1);

        see.execute("Test");

    }

    private static abstract class PercentileWatermarkGenerator<T> implements AssignerWithPeriodicWatermarks<T> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return null;
        }

        @Override
        public long extractTimestamp(T element, long previousElementTimestamp) {
            return 0;
        }

        public abstract long extractTimestamp(T element);
    }
}
