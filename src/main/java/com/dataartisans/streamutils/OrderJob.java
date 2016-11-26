package com.dataartisans.streamutils;

import com.dataartisans.eventsession.Event;
import com.dataartisans.eventsession.EventGenerator;
import com.dataartisans.eventsession.StreamShuffler;
import com.dataartisans.utils.ThroughputLogger;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Session window opened and closed based on events
 */
public class OrderJob {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        FlinkUtils.enableJMX(config);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment(1, config);

        ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.setParallelism(1);
        see.enableCheckpointing(1000);


        DataStream<Event> stream = see.addSource(new EventGenerator(pt)).setParallelism(1);
        stream = stream.flatMap(new StreamShuffler<Event>(15)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.milliseconds(60L)) {
            @Override
            public long extractTimestamp(Event element) {
                return element.id;
            }
        });
        stream = TimeSmoother.forStream(stream);
       // stream.print();


    //    stream.print();
         stream.flatMap(new ThroughputLogger(5_000_000L)).setParallelism(1);

        see.execute("Test");

    }


    /**
     * Orders events coming within a similar time-frame.
     *
     * Note: For using checkpoints, T must be serializable.
     */
    private static class TimeSmoother<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, Checkpointed<TreeMap<Long, T>> {

        private transient TreeMap<Long, T> tree;

        public static <T> DataStream<T> forStream(DataStream<T> stream) {
            if(stream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
                throw new IllegalArgumentException("This operator doesn't work with processing time");
            }
            return stream.transform("TimeSmoother", stream.getType(), new TimeSmoother<T>());
        }

        @Override
        public void open() throws Exception {
            if(tree == null) {
                // we are not restoring something here.
                tree = new TreeMap<>();
            }
            getMetricGroup().gauge("treeSize", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return tree.size();
                }
            });

            //  long now = getRuntimeContext().getCurrentProcessingTime();
            // System.out.println("registering timer at " + now);
            // getRuntimeContext().registerTimer(now + 6000L, this);
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            //System.out.println("Received element " + element);
            tree.put(element.getTimestamp(), element.getValue());
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processWatermark(Watermark mark) throws Exception {
            //System.out.println("Received watermark " + mark);
            long watermark = mark.getTimestamp();
            NavigableMap<Long, T> elementsLowerOrEqualsToWatermark = tree.headMap(watermark, true);
            Iterator<Map.Entry<Long, T>> iterator = elementsLowerOrEqualsToWatermark.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<Long, T> el = iterator.next();
                output.collect(new StreamRecord<>( el.getValue(), el.getKey()));
                iterator.remove();
            }
            output.emitWatermark(mark);
        }

        @Override
        public TreeMap<Long, T> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
            return tree;
        }

        @Override
        public void restoreState(TreeMap<Long, T> state) throws Exception {
            tree = state;
        }
    }
}
