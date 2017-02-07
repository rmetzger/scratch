package com.dataartisans.quantilewatermarks;

import com.clearspring.analytics.stream.quantile.QDigest;
import com.dataartisans.eventsession.Event;
import com.dataartisans.eventsession.EventGenerator;
import com.dataartisans.eventsession.StreamShuffler;
import com.dataartisans.streamutils.FlinkUtils;
import com.dataartisans.utils.ThroughputLogger;
import com.tdunning.math.stats.TDigest;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.XORShiftRandom;

import javax.annotation.Nullable;

/**
 * Session window opened and closed based on events
 */
public class WatermarkQualityScoreTest {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FlinkUtils.enableJMX(conf);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.getConfig().setAutoWatermarkInterval(1000);
        see.setParallelism(1);

        DataStream<Event> stream = see.addSource(new EventGenerator(pt)).setParallelism(1);
        stream = stream.flatMap(new StreamShuffler<Event>(2000)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.milliseconds(10)) {
            @Override
            public long extractTimestamp(Event element) {
                return element.id;
            }
        });

        WatermarkQualityScorer.score(stream);

    //    stream.print();
        stream.flatMap(new ThroughputLogger(2_000_000L)).setParallelism(1);

        see.execute("Test");

    }


    public static class WatermarkQualityScorer<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>{

        public static <T> void score(DataStream<T> stream) {
            stream.transform("Watermark Quality Scorer", stream.getType(), new WatermarkQualityScorer<T>());
        }

        private transient long currentWatermark;
        private transient long elementsSeen;
        private transient long elementsBehindWatermark;
        private transient long lastWatermark;

        @Override
        public void open() throws Exception {
            lastWatermark = Long.MIN_VALUE;
            this.currentWatermark = Long.MIN_VALUE; // everything wil always be before the WM
            MetricGroup metricGroup = this.getMetricGroup().addGroup("watermarkQuality");
            metricGroup.gauge("currentWatermark", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return currentWatermark;
                }
            });
            metricGroup.gauge("elementsSeen", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return elementsSeen;
                }
            });
            metricGroup.gauge("elementsBehindWatermark", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return elementsBehindWatermark;
                }
            });
            metricGroup.gauge("qualityScore", new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return elementsBehindWatermark/(double)elementsSeen;
                }
            });
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            this.elementsSeen++;
            if(element.getTimestamp() < currentWatermark) {
                this.elementsBehindWatermark++;
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            LOG.info("Final quality score " + elementsBehindWatermark/(double)elementsSeen +" elements behind " + elementsBehindWatermark +" of " + this.elementsSeen);
            this.currentWatermark = mark.getTimestamp();
            this.elementsSeen = 0;
            this.elementsBehindWatermark = 0;

            if(this.currentWatermark < this.lastWatermark) {
                throw new IllegalStateException("Current watermark is lower than last watermark");
            }
            this.lastWatermark = currentWatermark;
        }
    }
}
