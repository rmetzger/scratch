package com.dataartisans.streamutils;

import com.dataartisans.eventsession.Event;
import com.dataartisans.eventsession.EventGenerator;
import com.dataartisans.eventsession.StreamShuffler;
import com.dataartisans.utils.ThroughputLogger;
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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Session window opened and closed based on events
 *
 * Performance:
 *  Java TreeMap (disfunctional)
 *      With checkpointing: 1.750.000 el / second
 *      no checkpoints: 1.750.000 el / second
 *  Java + custom TreeMultiMap
 *      with checkpoints: 1.400.000 el / sec
 *      no checkpoints: 1.400.000 el / sec
 */
public class OrderJob {

    public static void main(String[] args) throws Exception {

//      //  TreesMultiset
//        TreeMultimap<Integer, String> a = TreeMultimap.create();
//        a.put(0, "null");
//        a.put(1, "one");
//        a.put(1, "test");
//        a.put(3, "three");
//        a.put(3, "4g");
//        // get lowest
//        Map.Entry<Integer, Collection<String>> first = a.asMap().firstEntry();
//        System.out.println("first = " + first);
//        System.out.println("a = " + a);
//        System.exit(0);

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
            implements OneInputStreamOperator<T, T>, Checkpointed<TreeMultiMap<Long, T>> {

        private transient TreeMultiMap<Long, T> tree;
        private int sizeLimit = 0;

        public TimeSmoother(int sizeLimit) {
            if(sizeLimit < 0) {
                throw new IllegalArgumentException("Size limit is negative");
            }
            this.sizeLimit = sizeLimit;
        }

        public static <T> DataStream<T> forStream(DataStream<T> stream) {
            return forStream(stream, 0);
        }

        public static <T> DataStream<T> forStream(DataStream<T> stream, int sizeLimit) {
            if(stream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
                throw new IllegalArgumentException("This operator doesn't work with processing time. " +
                        "The time characteristic is set to '" + stream.getExecutionEnvironment().getStreamTimeCharacteristic() + "'.");
            }
            return stream.transform("TimeSmoother", stream.getType(), new TimeSmoother<T>(sizeLimit));
        }

        @Override
        public void open() throws Exception {
            // if tree is set, we are restored.
            if(tree == null) {
                tree = new TreeMultiMap<>();
            }
            getMetricGroup().gauge("treeSize", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return tree.size();
                }
            });
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            if(sizeLimit > 0 && tree.size() > sizeLimit) {
                // emit lowest elements from tree
                Map.Entry<Long, List<T>> lowest = tree.firstEntry();
                for(T record: lowest.getValue()) {
                    output.collect(new StreamRecord<>(record, lowest.getKey()));
                }
                tree.remove(lowest.getKey());
            }
            tree.put(element.getTimestamp(), element.getValue());
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processWatermark(Watermark mark) throws Exception {
            long watermark = mark.getTimestamp();
            NavigableMap<Long, List<T>> elementsLowerOrEqualsToWatermark = tree.headMap(watermark, true);
            Iterator<Map.Entry<Long, List<T>>> iterator = elementsLowerOrEqualsToWatermark.entrySet().iterator();
            int removed = 0;
            while(iterator.hasNext()) {
                Map.Entry<Long, List<T>> el = iterator.next();
                for(T record: el.getValue()) {
                    output.collect(new StreamRecord<>(record, el.getKey()));
                    removed++;
                }
                iterator.remove();
            }
            tree.reportRemoved(removed);
            output.emitWatermark(mark);
        }

        @Override
        public TreeMultiMap<Long, T> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
            return tree;
        }

        @Override
        public void restoreState(TreeMultiMap<Long, T> state) throws Exception {
            tree = state;
        }
    }

    private static class TreeMultiMap<K, V> implements Serializable {

        private final TreeMap<K, List<V>> tree;
        private int size;

        public TreeMultiMap() {
            this.tree = new TreeMap<>();
            this.size = 0;
        }

        public int size() {
            return size;
        }

        public void remove(K key) {
            // this is not the cheapest thing
            size -= tree.get(key).size();
            tree.remove(key);
        }

        public void put(K key, V value) {
            size++;
            List<V> entry = tree.get(key);
            if(entry == null) {
                entry = new ArrayList<>();
                tree.put(key, entry);
            }
            entry.add(value);
        }

        public Map.Entry<K, List<V>> firstEntry() {
            return tree.firstEntry();
        }

        public NavigableMap<K, List<V>> headMap(K key, boolean inclusive) {
            return tree.headMap(key, inclusive);
        }

        public void reportRemoved(int removed) {
            this.size -= removed;
        }
    }
}
