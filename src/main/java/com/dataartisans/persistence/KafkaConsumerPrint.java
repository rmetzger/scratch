package com.dataartisans.persistence;

import kafka.consumer.ConsumerConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Utils;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Simple consumer which has one unchained mapper and is printing everything to
 * stdout
 */
public class KafkaConsumerPrint {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerPrint.class);

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final int sourcePar = params.getInt("sourcePar");
		final int sinkPar = params.getInt("sinkPar");
		final String topicName = params.get("topicName");
		final String zkConnect = params.get("zkConnect");

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.enableCheckpointing(500);
		see.setNumberOfExecutionRetries(15);
		see.getConfig().setGlobalJobParameters(params);
		
		Properties props = new Properties();
		props.setProperty("auto.commit.enable", "false");
		props.setProperty("group.id", "flink-kafka-consumer-topology");
		props.setProperty("zookeeper.connect", zkConnect);
		props.setProperty("auto.offset.reset", params.get("offsetReset"));
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
		}).disableChaining().flatMap(new RichFlatMapFunction<KafkaMessage, Integer>() {
			@Override
			public void flatMap(KafkaMessage kafkaMessage, Collector<Integer> collector) throws Exception {
				LOG.info("Got KafkaMessage: {}", kafkaMessage);
			}
		}).setParallelism(sinkPar);
		// this is going to be empty
		finalCount.print();

		see.execute("Kafka Consumer Topology");
	}

}
