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

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTopology {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTopology.class);

	public static void main(String[] args) throws Exception {
		final int sourcePar = Integer.valueOf(args[1]);
		final int sinkPar = Integer.valueOf(args[2]);
		final int log = Integer.valueOf(args[3]);
		final String topicName = args[4];
		final String zkConnect = args[5];
		final int numElements = Integer.valueOf(args[7]);
		int sleep = Integer.valueOf(args[8]);
		int numDuplicates = Integer.valueOf(args[9]);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.enableCheckpointing(500);
		see.setNumberOfExecutionRetries(15);
		
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
		DataStream<Integer> finalCount = inStream.map(new MapFunction<KafkaMessage, KafkaMessage>() {
			@Override
			public KafkaMessage map(KafkaMessage value) throws Exception {
				return value;
			}
		}).disableChaining().flatMap(new Checker(log, numElements, sleep, numDuplicates)).setParallelism(sinkPar);
		finalCount.print();

		see.execute("Kafka Consumer Topology");
	}

	public static class Checker extends RichFlatMapFunction<KafkaMessage, Integer>
			implements CheckpointedAsynchronously<Tuple2<Integer, int[]>> {

		int log;
		int numElements;
		private long sleep;
		private int numDuplicates;

		public Checker(int log, int numElements, long s, int numDuplicates) {
			this.log = log;
			this.numElements = numElements;
			this.sleep = s;
	//		checker = new int[numElements];
			this.numDuplicates = numDuplicates;
		}

		int count = 0;
	//	int[] checker;

		@Override
		public void flatMap(KafkaMessage value, Collector<Integer> col) throws Exception {
			getRuntimeContext().getLongCounter("counter").add(1L);

		//	checker[(int)value.offset]++;
			Thread.sleep(sleep);
			count++;
			if (count % log == 0) {
				LOG.info("Received {} elements from Kafka.", count);
			}
			if(count == numElements) {
				LOG.info("Final count "+count);
		/*		for(int i = 0; i < checker.length; i++) {
					if(checker[i] > numDuplicates) {
						throw new RuntimeException("Saw "+checker[i]+" duplicates, but only "+numDuplicates+" were allowed on "+i+" in "+Arrays.toString(checker));
					}
				} */
				col.collect(count);
			}
		}

		@Override
		public Tuple2<Integer, int[]> snapshotState(long checkpointId, long timestamp) throws Exception {
			LOG.info("Checkpointing state: "+count+" on checkpoint "+checkpointId);
			return new Tuple2<Integer, int[]>(count, /*Arrays.copyOf(checker, checker.length) */ null);
		}

		@Override
		public void restoreState(Tuple2<Integer, int[]> oldState) {
			LOG.info("Restarting from old state "+oldState);
			count = oldState.f0;
			//checker = Arrays.copyOf(oldState.f1, oldState.f1.length);
		}
	}
}
