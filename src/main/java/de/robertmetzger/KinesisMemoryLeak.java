package de.robertmetzger;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;


import java.net.URL;
import java.util.Properties;

public enum KinesisMemoryLeak {
    ;

    public static void main(String[] args) throws Exception {
        final String streamName = "kinesis_stream_name";
        Properties producerConfig = new Properties();

        producerConfig.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.BASIC.name());
        producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
        producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "fakeid");
        producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "fakekey");
        producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567");
        String kinesisUrl = producerConfig.getProperty(AWSConfigConstants.AWS_ENDPOINT);
        if (kinesisUrl != null) {
            URL url = new URL(kinesisUrl);
            producerConfig.put("KinesisEndpoint", url.getHost());
            producerConfig.put("KinesisPort", Integer.toString(url.getPort()));
            producerConfig.put("VerifyCertificate", "false");
        }

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        //see.setRestartStrategy(RestartStrategies.fixedDelayRestart(10_000, 0L));
        see.setRestartStrategy(RestartStrategies.noRestart());
        DataStream<String> source = see.addSource(
            new SourceFunction<String>() {

                private boolean running = true;

                @Override
                public void run(SourceContext<String> ctx) throws Exception {
                    // load some additional classes:
                    AvroInputFormat ai = new AvroInputFormat(new Path("file:///tmp/"), String.class);
                    throw new RuntimeException("Artifical test failure");
                   /* long cnt = 0;
                    while (running) {
                        ctx.collect("kinesis-" + cnt);
                        cnt++;
                        if (cnt % 2 == 0) {
                            throw new RuntimeException("Artifical test failure");
                            // Thread.sleep(10);
                        }
                    } */
                }

                @Override
                public void cancel() {
                    running = false;
                }
            });



        FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(new SimpleStringSchema(), producerConfig);
        kinesis.setFailOnError(true);
        kinesis.setDefaultStream(streamName);
        kinesis.setDefaultPartition("0");

        source.addSink(kinesis); // new DiscardingSink<>()
        //source.addSink(new DiscardingSink<>());

        see.execute("please kill me");
    }
}
