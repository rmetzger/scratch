package de.robertmetzger;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;

public class DIYTwoPhaseCommitExampleJob {

    private static final Logger logger = LoggerFactory.getLogger(SinkFunctionExampleJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10_000L);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/checkpoint-storage"));
        env.setParallelism(2);
        env.setMaxParallelism(10);

        DataStream<String> dataStream = env.addSource(new DataGen()).setParallelism(1);

        dataStream.addSink(new TwoPhaseCommitSink());
        env.executeAsync();
    }

    private enum SinkState {
        COLLECTING_DATA,
        COMMITTABLE_COMPLETE,
        EMIT_DOWNSTREAM,
    }

    /**
     * I first tried implementing a sink that commits on "notifyCheckpointComplete", but the problem is that we have no guarantee
     * that this method get's called. I didn't see a solution to this problem
     *
     * Next approach: commit the previous checkpoint's data in the snapshot method.
     */
    private static class TwoPhaseCommitSink extends RichSinkFunction<String> implements CheckpointedFunction /*, CheckpointListener */ {

        private ListState<String> committableState;
        @GuardedBy("lock")
        private SinkState state = SinkState.COLLECTING_DATA;
        private long fromCheckpoint = -1L;
        private transient Object lock = new Object();

        @Override
        public void open(Configuration parameters) throws Exception {
            state = SinkState.COLLECTING_DATA;
            logger.info("state changed to {}", state);
            lock = new Object();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            if (state == SinkState.COMMITTABLE_COMPLETE) {
                // we need to wait until the checkpoint is complete.
                while (true) {
                    synchronized (lock) {
                        lock.wait();
                        if (state == SinkState.EMIT_DOWNSTREAM) {
                            break;
                        }
                    }
                }
            }
            if (state == SinkState.EMIT_DOWNSTREAM) {
                // we are ready to emit downstream
                logger.info("Emitting downstream from checkpoint {}", fromCheckpoint);
                long cnt = 0;
                for (String rec: committableState.get()) {
                    cnt++;
                }
                committableState.clear();
                logger.info("emitted {} records downstream", cnt);
                state = SinkState.COLLECTING_DATA;
                logger.info("state changed to {}", state);
                // now, very important, collect "value" into state again.
            }
            committableState.add(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (state != SinkState.COLLECTING_DATA) {
                throw new IllegalStateException("Unexpected state: " + state);
            }
            state = SinkState.COMMITTABLE_COMPLETE;
            logger.info("state changed to {}", state);
            logger.info("Operator checkpointed");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> descriptor =
                    new ListStateDescriptor<>(
                            "committable",
                            TypeInformation.of(new TypeHint<String>() {}));

            committableState = context.getOperatorStateStore().getListState(descriptor);
            if (context.isRestored()) {
                synchronized (lock) {
                    state = SinkState.EMIT_DOWNSTREAM;
                    fromCheckpoint = context.getRestoredCheckpointId().getAsLong();
                    logger.info("state changed to {}", state);
                }
            }
            logger.info("State initialized");
        }

        /*@Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            logger.info("Checkpoint completed");
            synchronized (lock) {
                if (state != SinkState.COMMITTABLE_COMPLETE) {
                    throw new IllegalStateException("Unexpected state: " + state);
                }
                state = SinkState.EMIT_DOWNSTREAM;
                fromCheckpoint = checkpointId;
                logger.info("state changed to {}", state);
                lock.notifyAll();
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            CheckpointListener.super.notifyCheckpointAborted(checkpointId);
        } */
    }


    private static class DataGen extends RichParallelSourceFunction<String> {
        private boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long i = 0;
            while(running) {
                Thread.sleep(1);
                ctx.collect("sample:" + i);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
