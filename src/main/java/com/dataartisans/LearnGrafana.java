package com.dataartisans;

import com.dataartisans.flinktraining.exercises.datastream_java.utils.influxdb.DataPoint;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.influxdb.InfluxDBSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 *
 */
public class LearnGrafana {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<DataPoint<Double>> stream = see.addSource(new SourceFunction<DataPoint<Double>>() {
            public boolean running = true;

            @Override
            public void run(SourceContext<DataPoint<Double>> ctx) throws Exception {
                Random RND = new Random(1337);
                while(running) {
                    DataPoint<Double> point = new DataPoint<>(System.currentTimeMillis(), RND.nextGaussian());
                    ctx.collect(point);
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        ParameterTool parameters = ParameterTool.fromArgs(args);
        stream.addSink(new InfluxDBSink<DataPoint<Double>>("gauss", parameters)).setParallelism(1);

        see.execute("Grafana");
    }
}
