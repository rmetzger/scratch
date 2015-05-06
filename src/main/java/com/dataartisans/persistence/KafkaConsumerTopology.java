package com.dataartisans.persistence;

import kafka.consumer.ConsumerConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Utils;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Properties;

public class KafkaConsumerTopology {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTopology.class);

	public static void main(String[] args) throws Exception {
		final int sourcePar = Integer.valueOf(args[1]);
		final int sinkPar = Integer.valueOf(args[2]);
		final int log = Integer.valueOf(args[3]);
		final String topicName = args[4];
		final String zkConnect = args[5];
		final long numElements = Long.valueOf(args[7]);
		int sleep = Integer.valueOf(args[8]);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.enableCheckpointing(500);
		see.setNumberOfExecutionRetries(0);
		
		Properties props = new Properties();
		props.setProperty("auto.commit.enable", "false");
		props.setProperty("group.id", "flink-kafka-consumer-topology");
		props.setProperty("zookeeper.connect", zkConnect);
		props.setProperty("auto.offset.reset", args[6]);
		final ConsumerConfig consumerConfig = new ConsumerConfig(props);
		DataStream<KafkaMessage> inStream = see.addSource(new PersistentKafkaSource<KafkaMessage>(topicName,
				new Utils.TypeInformationSerializationSchema<KafkaMessage>(new KafkaMessage(), see.getConfig()),
				consumerConfig)).setParallelism(sourcePar);

		// source --> map -->  (discarding) filter (unchained)
		DataStream<Long> finalCount = inStream.map(new MapFunction<KafkaMessage, KafkaMessage>() {
			@Override
			public KafkaMessage map(KafkaMessage value) throws Exception {
				return value;
			}
		}).disableChaining().flatMap(new Checker(log, numElements, sleep)).setParallelism(sinkPar);
		finalCount.print();

		see.execute("Kafka Consumer Topology");
	}

	public static class Checker extends RichFlatMapFunction<KafkaMessage, Long> implements CheckpointedAsynchronously<Tuple2<Long, BitSet>> {

		int log;
		long numElements;
		private long sleep;

		public Checker(int log, long numElements, long s) {
			this.log = log;
			this.numElements = numElements;
			this.sleep = s;
		}

		long count = 0;
		BitSet checker = new BitSet(16000); // thats only the initial size

		@Override
		public void flatMap(KafkaMessage value, Collector<Long> col) throws Exception {
			if (checker.get((int) value.offset)) {
				LOG.warn("Bit {} in {} already set", value.offset, checker);
				throw new RuntimeException("Bit " + value.offset + " already set");
			}
			checker.set((int) value.offset);
			Thread.sleep(sleep);
			count++;
			if (count % log == 0) {
				LOG.info("Received {} elements from Kafka. Highest element seen {}", count, checker.nextSetBit(0));
			}
			if(count == numElements) {
				LOG.info("Final count "+count);
				col.collect(count);
			}
		}

		@Override
		public Tuple2<Long, BitSet> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Checkpointing state: "+count+" on checkpoint "+checkpointId);
			return new Tuple2<Long, BitSet>(count, (BitSet) checker.clone());
		}

		@Override
		public void restoreState(Tuple2<Long, BitSet> oldState) {
			LOG.info("Restarting from old state "+oldState);
			count = oldState.f0;
			checker = (BitSet) oldState.f1.clone();
		}
	}
}
