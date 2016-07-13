/*

  Copyright 2016 data Artisans GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.dataartisans;


import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.ExceptionProxy;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09.DEFAULT_POLL_TIMEOUT;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09.KEY_POLL_TIMEOUT;

/**
 * This special Apache Kafka consumer does not provide exactly-once or ordering guarantees.
 * It relies on Kafka's consumer group mechanism to split up work and to scale up and down.
 *
 * The consumer allows to scale a (stateless) streaming job up and down because the consumer
 * group mechanism is going to take care of the partition assignment.
 *
 * This class contains a lot of copied code from Apache Flink.
 *
 */
public class FlinkKafkaGroupConsumer<T> extends RichSourceFunction<T>
		implements Runnable, CheckpointListener, Checkpointed<HashMap<TopicPartition, OffsetAndMetadata>> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaGroupConsumer.class);

	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

	private final KeyedDeserializationSchema<T> schema;
	private final Properties properties;
	private final long pollTimeout;

	// This consumer supports two kinds of topic definitions: pattern-based xor a list of topic.
	private List<String> topics;
	private Pattern topicPattern;

	// ------------------------ Runtime fields -----------------------------------

	/** Data for pending but uncommitted checkpoints */
	private final LinkedMap pendingCheckpoints = new LinkedMap();
	private transient KafkaConsumer<byte[], byte[]> kafkaConsumer;
	private transient ExceptionProxy errorHandler;
	private transient boolean running;
	private boolean useMetrics;
	private transient SourceContext<T> sourceContext;
	private List<KafkaTopicPartitionState<TopicPartition>> currentPartitions = new ArrayList<>();
	private final SerializableObject consumerLock = new SerializableObject();

	public FlinkKafkaGroupConsumer(Pattern topicPattern, KeyedDeserializationSchema<T> schema, Properties properties) {
		this(schema, properties);
		this.topicPattern = topicPattern;
	}

	public FlinkKafkaGroupConsumer(List<String> topics, KeyedDeserializationSchema<T> schema, Properties properties) {
		this(schema, properties);
		this.topics = topics;
	}

	/**
	 * Internal constructor
	 */
	private FlinkKafkaGroupConsumer(KeyedDeserializationSchema<T> schema, Properties properties) {
		this.schema = Preconditions.checkNotNull(schema, "Passed DeserializationSchema was null");
		this.properties = Preconditions.checkNotNull(properties, "Passed Properties were null");
		setDeserializer(this.properties);
	//	this.useMetrics = !Boolean.valueOf(properties.getProperty(FlinkKafkaConsumerBase.KEY_DISABLE_METRICS, "false"));
		validateKafkaProperties(properties);
		// configure the polling timeout
		try {
			if (properties.containsKey(KEY_POLL_TIMEOUT)) {
				this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
			} else {
				this.pollTimeout = DEFAULT_POLL_TIMEOUT;
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
		}
	}



	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.running = true;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		this.errorHandler = new ExceptionProxy(Thread.currentThread());
		this.sourceContext = ctx;

		// rather than running the main fetch loop directly here, we spawn a dedicated thread
		// this makes sure that no interrupt() call upon canceling reaches the Kafka consumer code
		Thread runner = new Thread(this, "Group Kafka 0.9 Fetcher for " + getRuntimeContext().getTaskNameWithSubtasks());
		runner.setDaemon(true);
		runner.start();

		try {
			runner.join();
		} catch (InterruptedException e) {
			// may be the result of a wake-up after an exception. we ignore this here and only
			// restore the interruption state
			Thread.currentThread().interrupt();
		}

		// make sure we propagate any exception that occurred in the concurrent fetch thread,
		// before leaving this method
		this.errorHandler.checkAndThrowException();
	}

	@Override
	public void run() {
		try {
			// Initialize consumer
			this.kafkaConsumer = new KafkaConsumer<>(this.properties);
			if (this.topicPattern != null && this.topics != null) {
				throw new IllegalStateException("Topic pattern and topics list has been set");
			}
			// Rebalance listener is maintaining the "currentPartitions" list. Access is synchronized over the "currentPartitions"
			// to avoid concurrent modifications. The rebalancing should happen fairly rare.
			ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					LOG.info("Revoked partitions {}", partitions);
					synchronized (currentPartitions) {
						// remove partitions from TopicPartition list
						Iterator<KafkaTopicPartitionState<TopicPartition>> currentPartitionsIterator = currentPartitions.iterator();
						while (currentPartitionsIterator.hasNext()) {
							if (partitions.contains(currentPartitionsIterator.next().getKafkaPartitionHandle())) {
								currentPartitionsIterator.remove();
							}
						}
					}
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					LOG.info("Assigned partitions {}", partitions);
					synchronized (currentPartitions) {
						for (TopicPartition partition : partitions) {
							if (!contains(currentPartitions, partition)) {
								currentPartitions.add(new KafkaTopicPartitionState<>(new KafkaTopicPartition(partition.topic(), partition.partition()), partition));
							}
						}
					}
				}
			};
			if (this.topicPattern != null) {
				this.kafkaConsumer.subscribe(this.topicPattern, rebalanceListener);
			}
			if (this.topics != null) {
				this.kafkaConsumer.subscribe(this.topics, rebalanceListener);
			}

// Uncomment once kafka metrics are added to Apache Flink (1.1)
//			if (useMetrics) {
//				final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaGroupConsumer");
//				// register Kafka metrics to Flink
//				Map<MetricName, ? extends Metric> metrics = kafkaConsumer.metrics();
//				if (metrics == null) {
//					// MapR's Kafka implementation returns null here.
//					LOG.info("Consumer implementation does not support metrics");
//				} else {
//					// we have Kafka metrics, register them
//					for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
//						String name = MetricUtils.cleanMetricName(metric.getKey().name());
//						kafkaMetricGroup.gauge(name, new KafkaMetricGetter(metric.getValue()));
//					}
//				}
//			}

			// main fetch loop
			while (running) {
				// get the next batch of records
				final ConsumerRecords<byte[], byte[]> records;
				try {
					synchronized (consumerLock) {
						records = this.kafkaConsumer.poll(pollTimeout);
					}
				} catch (WakeupException we) {
					if (running) {
						throw we;
					} else {
						continue;
					}
				}

				synchronized (currentPartitions) {
					for (KafkaTopicPartitionState<TopicPartition> tp : currentPartitions) {
						for (ConsumerRecord<byte[], byte[]> record : records.records(tp.getKafkaPartitionHandle())) {
							T value = schema.deserialize(
									record.key(), record.value(),
									record.topic(), record.partition(), record.offset());

							if (schema.isEndOfStream(value)) {
								// end of stream signaled
								running = false;
								break;
							}
							synchronized (sourceContext.getCheckpointLock()) {
								sourceContext.collect(value);
								tp.setOffset(record.offset());
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			if (running) {
				running = false;
				errorHandler.reportError(t);
			} else {
				LOG.debug("Stopped ConsumerThread threw exception", t);
			}
		} finally {
			try {
				synchronized (consumerLock) {
					kafkaConsumer.close();
				}
			} catch (Throwable t) {
				LOG.warn("Error while closing Kafka 0.9 consumer", t);
			}
		}
	}

	@Override
	public void cancel() {
		// flag the main thread to exit
		running = false;

		// NOTE:
		//   - We cannot interrupt the runner thread, because the Kafka consumer may
		//     deadlock when the thread is interrupted while in certain methods
		//   - We cannot call close() on the consumer, because it will actually throw
		//     an exception if a concurrent call is in progress

		// make sure the consumer finds out faster that we are shutting down
		if (kafkaConsumer != null) {
			kafkaConsumer.wakeup();
		}
	}

	// ----------------------- At-least-once guarantees  ------------------------------------



	@Override
	public HashMap<TopicPartition, OffsetAndMetadata> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
			return null;
		}

		HashMap<TopicPartition, OffsetAndMetadata> stateToCheckpoint;
		synchronized (currentPartitions) {
			stateToCheckpoint = new HashMap<>(currentPartitions.size());
			for(KafkaTopicPartitionState<TopicPartition> partitionState: currentPartitions) {
				if(!partitionState.isOffsetDefined()) { // ignore undefined offsets
					continue;
				}
				if(stateToCheckpoint.put(partitionState.getKafkaPartitionHandle(), new OffsetAndMetadata(partitionState.getOffset())) != null) {
					throw new IllegalStateException("currentPartitions are not properly maintained: Duplicate partitions: " + currentPartitions);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. Offsets: {}, checkpoint id: {}, timestamp: {}",
					stateToCheckpoint, checkpointId, checkpointTimestamp);
		}

		// the map cannot be asynchronously updated, because only one checkpoint call can happen
		// on this function at a time: either snapshotState() or notifyCheckpointComplete()
		pendingCheckpoints.put(checkpointId, stateToCheckpoint);

		// truncate the map, to prevent infinite growth
		while (pendingCheckpoints.size() > MAX_NUM_PENDING_CHECKPOINTS) {
			pendingCheckpoints.remove(0);
		}

		return stateToCheckpoint;
	}

	@Override
	public void restoreState(HashMap<TopicPartition, OffsetAndMetadata> restoredOffsets) {
		// nothing to restore
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (!running) {
			LOG.debug("notifyCheckpointComplete() called on closed source");
			return;
		}

		final KafkaConsumer<?, ?> fetcher = this.kafkaConsumer;
		if (fetcher == null) {
			LOG.debug("notifyCheckpointComplete() called on uninitialized source");
			return;
		}

		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing offsets to Kafka for checkpoint " + checkpointId);
		}

		try {
			final int posInMap = pendingCheckpoints.indexOf(checkpointId);
			if (posInMap == -1) {
				LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
				return;
			}

			@SuppressWarnings("unchecked")
			HashMap<TopicPartition, OffsetAndMetadata> checkpointOffsets =
					(HashMap<TopicPartition, OffsetAndMetadata>) pendingCheckpoints.remove(posInMap);

			// remove older checkpoints in map
			for (int i = 0; i < posInMap; i++) {
				pendingCheckpoints.remove(0);
			}

			if (checkpointOffsets == null || checkpointOffsets.size() == 0) {
				LOG.debug("Checkpoint state was empty.");
				return;
			}
			synchronized (consumerLock) {
				fetcher.commitSync(checkpointOffsets);
			}
		}
		catch (Exception e) {
			if (running) {
				throw e;
			}
			// else ignore exception if we are no longer running
		}
	}

	// ----------------------- Utilities ------------------------------------

	private boolean contains(List<KafkaTopicPartitionState<TopicPartition>> partitions, TopicPartition toCheck) {
		for(KafkaTopicPartitionState<TopicPartition> partition: partitions) {
			if(partition.getKafkaPartitionHandle().equals(toCheck)) {
				return true;
			}
		}
		return false;
	}

	private void validateKafkaProperties(Properties properties) {
		Class<ConsumerConfig> ccClass = ConsumerConfig.class;
		try {
			Constructor<ConsumerConfig> cTor = ccClass.getDeclaredConstructor(Map.class);
			cTor.setAccessible(true);
			cTor.newInstance(properties);
		} catch (InvocationTargetException errorFromCtor) {
			throw new RuntimeException("Invalid Kafka properties passed", errorFromCtor.getCause());
		} catch (Exception e) {
			throw new RuntimeException("Error while validating Kafka properties", e);
		}
	}

	private static void setDeserializer(Properties props) {
		String deSerName = ByteArrayDeserializer.class.getCanonicalName();
		Object keyDeSer = props.get("key.deserializer");
		Object valDeSer = props.get("value.deserializer");
		if(keyDeSer != null && !keyDeSer.equals(deSerName)) {
			LOG.warn("Ignoring configured key DeSerializer ({})", "key.deserializer");
		}

		if(valDeSer != null && !valDeSer.equals(deSerName)) {
			LOG.warn("Ignoring configured value DeSerializer ({})", "value.deserializer");
		}

		props.put("key.deserializer", deSerName);
		props.put("value.deserializer", deSerName);
	}

}
