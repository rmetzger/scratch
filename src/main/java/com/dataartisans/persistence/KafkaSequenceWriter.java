package com.dataartisans.persistence;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSequenceWriter {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaSequenceWriter.class);

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(params);

		final int bytes = params.getInt("bytes");
		final int sourcePar = params.getInt("sourcePar");
		final int sinkPar = params.getInt("sinkPar");
		final int sleep = params.getInt("sleep");
		final String topicName = params.get("topicName");
		final String brokerList = params.get("brokers");
		final long elementCount = params.getLong("elementCount", 100000L);

		DataStream<KafkaMessage> data = see.addSource(new RichParallelSourceFunction<KafkaMessage>() {
			boolean running = true;

			@Override
			public void run(SourceContext<KafkaMessage> collector) throws Exception {
				long count = 0;
				int part = getRuntimeContext().getIndexOfThisSubtask();
				byte[] data = new byte[bytes];
				while (running) {
					if(count == elementCount) {
						break;
					}
					final KafkaMessage msg = new KafkaMessage(count++, part, data);
					collector.collect(msg);
					LOG.info("wrote message {}", msg);
					Thread.sleep(sleep);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		}).setParallelism(sourcePar);

		SerializationSchema<KafkaMessage, byte[]> serSchema = new TypeInformationSerializationSchema<KafkaMessage>((TypeInformation<KafkaMessage>) TypeExtractor.createTypeInfo(KafkaMessage.class), see.getConfig());
		DataStreamSink<KafkaMessage> sink = data.addSink(new FlinkKafkaProducer<KafkaMessage>(brokerList, topicName, serSchema)).setParallelism(sinkPar);

		see.execute("Kafka sequence writer");
	}
}
