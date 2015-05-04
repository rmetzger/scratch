package com.dataartisans.persistence;

import kafka.consumer.ConsumerConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.connectors.kafka.Utils;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
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

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
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
		inStream.map(new MapFunction<KafkaMessage, KafkaMessage>() {
			@Override
			public KafkaMessage map(KafkaMessage value) throws Exception {
				return value;
			}
		}).setChainingStrategy(StreamOperator.ChainingStrategy.NEVER).filter(new RichFilterFunction<KafkaMessage>() {
			long count = 0;
			BitSet checker = new BitSet(16000); // thats only the initial size

			@Override
			public boolean filter(KafkaMessage value) throws Exception {
				if (checker.get((int) value.offset)) {
					LOG.warn("Bit {} in {} already set", value.offset, checker);
					throw new RuntimeException("Bit " + value.offset + " already set");
				}
				count++;
				if (count % log == 0) {
					LOG.info("Received {} elements from Kafka. Highest element seen {}", count, checker.nextSetBit(0));
				}
				return false;
			}
		}).setParallelism(sinkPar);

		see.execute("Kafka Consumer Topology");
	}
}
