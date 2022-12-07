package de.robertmetzger;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
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
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

public class NextDIYTwoPhaseCommitExampleJob {

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

    private enum Target {
        BLUE, GREEN;
    }
    /**
     * I first tried implementing a sink that commits on "notifyCheckpointComplete", but the problem is that we have no guarantee
     * that this method get's called. I didn't see a solution to this problem
     *
     * Next approach: commit the previous checkpoint's data in the snapshot method.
     */
    private static class TwoPhaseCommitSink extends RichSinkFunction<String> implements CheckpointedFunction /*, CheckpointListener */ {

        private ListState<String> blue;
        private ListState<String> green;
        private ListState<Target> currentTarget;
        private long fromCheckpoint = 1L;
        private Target writeTo;

        @Override
        public void open(Configuration parameters) throws Exception {
            writeTo = Target.BLUE;
            logger.info("open: writeTo = {}", writeTo);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            if (writeTo == Target.BLUE) {
                blue.add(value);
            }
            if (writeTo == Target.GREEN) {
                green.add(value);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (writeTo == Target.BLUE) {
                // blue is the currently pending commitable, so we emit records in green:
                drain(green, fromCheckpoint);
                fromCheckpoint = context.getCheckpointId();
                green.clear();
                writeTo = Target.GREEN;
                logger.info("snapshotState: writeTo = {}", writeTo);
                return;
            }
            if (writeTo == Target.GREEN) {
                drain(blue, fromCheckpoint);
                fromCheckpoint = context.getCheckpointId();
                blue.clear();
                writeTo = Target.BLUE;
                logger.info("snapshotState: writeTo = {}", writeTo);
                return;
            }

        }

        private void drain(ListState<String> input, long checkpointId) throws Exception {
            long cnt = 0;
            // to validate the output, cat all files together, sort them: sort -t : -k 2 -g all > all.sorted
            BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/output/target-"+checkpointId + "-" + getRuntimeContext().getIndexOfThisSubtask()));
            for (String rec: input.get()) {
                writer.write(rec);
                writer.write("\n");
                cnt++;
            }
            writer.close();
            logger.info("emitted {} records downstream", cnt);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> blueDescriptor =
                    new ListStateDescriptor<>(
                            "blue",
                            TypeInformation.of(new TypeHint<String>() {}));

            blue = context.getOperatorStateStore().getListState(blueDescriptor);

            ListStateDescriptor<String> greenDescriptor =
                    new ListStateDescriptor<>(
                            "green",
                            TypeInformation.of(new TypeHint<String>() {}));
            green = context.getOperatorStateStore().getListState(greenDescriptor);

            ListStateDescriptor<Target> targetDescriptor =
                    new ListStateDescriptor<>(
                            "currTarget",
                            TypeInformation.of(new TypeHint<Target>() {}));
            currentTarget = context.getOperatorStateStore().getListState(targetDescriptor);
            if (context.isRestored()) {
                List<Target> targetList = Lists.newArrayList(currentTarget.get().iterator());
                if (targetList.size() > 1) {
                    throw new IllegalStateException("You can not rescale this op");
                }
                if (targetList.size() == 1) {
                    writeTo = targetList.get(0);
                    logger.info("initializeState: writeTo = {}", writeTo);
                    fromCheckpoint = context.getRestoredCheckpointId().getAsLong();
                }
            }

            logger.info("State initialized");
        }

    }


    private static class DataGen extends RichParallelSourceFunction<String> {
        private boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long i = 0;
            while(running) {
                // Thread.sleep(1);
                if (i % 10_000L == 0) {
                    Thread.sleep(100);
                }
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
