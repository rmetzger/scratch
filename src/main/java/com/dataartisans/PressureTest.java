package com.dataartisans;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by robert on 5/4/15.
 */
public class PressureTest {

	private static final Logger LOG = LoggerFactory.getLogger(PressureTest.class);

	public static class Workload {
		private byte[] data;
		private int partition;
		private long element;

		public Workload() {

		}

		public Workload(byte[] data, int partition, long element) {
			this.data = data;
			this.partition = partition;
			this.element = element;
		}

		public byte[] getData() {
			return data;
		}

		public void setData(byte[] data) {
			this.data = data;
		}

		public int getPartition() {
			return partition;
		}

		public void setPartition(int partition) {
			this.partition = partition;
		}

		public long getElement() {
			return element;
		}

		public void setElement(long element) {
			this.element = element;
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final int mb = Integer.valueOf(args[0]);
		final int dataSize = 1024 * 1024 * mb; // MB
		final int sourcePar = Integer.valueOf(args[1]);
		final int sinkPar = Integer.valueOf(args[2]);
		final int logFreq = Integer.valueOf(args[3]);
		DataStream<Workload> data = see.addSource(new RichParallelSourceFunction<Workload>() {

			public boolean running = true;

			@Override
			public void run(SourceContext<Workload> collector) throws Exception {
				long element = 0;
				final int partition = getRuntimeContext().getIndexOfThisSubtask();
				final byte[] data = new byte[dataSize];
				while (running) {
					if(element % logFreq == 0) {
						LOG.info("Elements processed {}, data send {} GB", element, ((dataSize*element)/(1024*1024*1024)) );
					}
					collector.collect(new Workload(data, partition, element++));
				}
			}

			@Override
			public void cancel() {
				this.running = false;
			}
		}).setParallelism(sourcePar);

		DataStream<Workload> out = data.broadcast().filter(new RichFilterFunction<Workload>() {
			long elements = 0;
			@Override
			public boolean filter(Workload value) throws Exception {
				elements++;
				if(elements % logFreq == 0) {
					LOG.info("Elements received {}, data received {} GB", elements, ((dataSize*elements)/(1024*1024*1024)) );
				}
				return false;
			}
		}).setParallelism(sinkPar);

		see.execute("Data flusher");
	}
}
