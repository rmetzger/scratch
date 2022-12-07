package de.robertmetzger;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class TwoPhaseCommitExampleJob {

    private static final Logger logger = LoggerFactory.getLogger(SinkFunctionExampleJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(5000L);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/checkpoint-storage"));
        env.setParallelism(2);
        env.setMaxParallelism(10);

        /*KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(pt.getRequired("kafka.bootstrap"))
                .setTopics(pt.getRequired("kafka.topics"))
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source"); */
        DataStream<String> dataStream = env.addSource(new DataGen());

        //dataStream.addSink(new TwoPhaseCommitSink());
        dataStream.print();
        env.executeAsync();
    }

    private static class DataGen extends RichParallelSourceFunction<String> {
        private boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(running) {
                Thread.sleep(500);
                ctx.collect("sample");
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class TwoPhaseCommitSink<T> implements TwoPhaseCommittingSink<T, Committable> {
        @Override
        public PrecommittingSinkWriter<T, Committable> createWriter(InitContext context) throws IOException {
            return null;
        }

        @Override
        public Committer<Committable> createCommitter() throws IOException {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<Committable> getCommittableSerializer() {
            return null;
        }
    }

    private static class Committable {

    }
}
