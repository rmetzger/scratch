package com.dataartisans.persistence;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.Utils;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Collector;


public class KafkaSequenceWriter {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final int bytes = Integer.valueOf(args[0]);
		final int sourcePar = Integer.valueOf(args[1]);
		final int sinkPar = Integer.valueOf(args[2]);
		final int sleep = Integer.valueOf(args[3]);
		final String topicName = args[4];
		final String brokerList = args[5];
		final long elementCount = Long.getLong(args[6]);
		DataStream<KafkaMessage> data = see.addSource(new RichParallelSourceFunction<KafkaMessage>() {

			boolean running = true;

			@Override
			public void run(Collector<KafkaMessage> collector) throws Exception {
				long count = 0;
				int part = getRuntimeContext().getIndexOfThisSubtask();
				byte[] data = new byte[bytes];
				while (running) {
					if(count == elementCount) {
						break;
					}
					collector.collect(new KafkaMessage(count++, part, data));
					Thread.sleep(sleep);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		}).setParallelism(sourcePar);

		SerializationSchema<KafkaMessage, byte[]> serSchema = new Utils.TypeInformationSerializationSchema<KafkaMessage>(new KafkaMessage(), see.getConfig());
		DataStream<KafkaMessage> sink = data.addSink(new KafkaSink<KafkaMessage>(brokerList, topicName, serSchema)).setParallelism(sinkPar);

		see.execute("Kafka sequence writer");
	}
}
