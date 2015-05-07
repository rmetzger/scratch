# Experiment documentation

- Bring up a HDP 2.2 cluster in GCE using bdutil.



Start flink yarn session

```
export YARN_CONF_DIR=/etc/hadoop/conf/

./bin/yarn-session.sh -n 5 -tm 1024 -Dyarn.maximum-failed-containers=500 -Dyarn.application-attempts=1 -Dtaskmanager.memory.fraction=0.001 -Dexecution-retries.default=4 -Dexecution-retries.delay=10s

```

Create topics: (killer-topic, test-topic)
```
/usr/hdp/2.2.3.0-2611/kafka/bin/kafka-topics.sh --create --topic abtest-topic --partitions 1 --replication-factor 1 --zookeeper hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181

```


Start consuming test topology:
```
../flink/build-target/bin/flink run -p 1 -c com.dataartisans.AtLeastOnceTesterTopology ./target/kafka-datagen-1.0-SNAPSHOT.jar hdp22-m.c.astral-sorter-757.internal test-topic hdfs://hdp22-m.c.astral-sorter-757.internal:8020/user/robert/kafka-out killer-topic 1 1 1
```

start data generation topology:

```
../flink/build-target/bin/flink run -p 1 -c com.dataartisans.KafkaDataGenerator target/kafka-datagen-1.0-SNAPSHOT.jar 1 test-topic  hdp22-m.c.astral-sorter-757.internal:6667 500
```


#####################################################################

Second test:
Topic with 5 partitions

Reader with parallelism of 10.

## create topic (5 partitions)
/usr/hdp/2.2.0.0-2041/kafka/bin/kafka-topics.sh --create --topic five-topic -partitions 5 --replication-factor 1 --zookeeper hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181


## start reader
../flink/build-target/bin/flink run -p 5 -c com.dataartisans.AtLeastOnceTesterTopology ./target/kafka-datagen-1.0-SNAPSHOT.jar hdp22-m.c.astral-sorter-757.internal five-topic hdfs://hdp22-m.c.astral-sorter-757.internal:8020/user/robert/kafka-out killer-topic 5 5 5

## start 5 senders

../flink/build-target/bin/flink run -p 5 -c com.dataartisans.KafkaDataGenerator target/kafka-datagen-1.0-SNAPSHOT.jar 5 five-topic  hdp22-m.c.astral-sorter-757.internal:6667 50


## start console consumer for topic:
/usr/hdp/current/kafka/bin/kafka-console-consumer.sh --zookeeper hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181 --topic five-topic


###################### NEW TESTS ######################################

Zookeeper connect: hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181


./bin/flink run -c com.dataartisans.persistence.OffsetUtil /home/robert/scratch/target/kafka-datagen-1.0-SNAPSHOT.jar get hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181 flink-kafka-consumer-topology test 0

export YARN_CONF_DIR=/etc/hadoop/conf
./bin/yarn-session.sh -n 2 -s 4 -tm 768 -jm 768 -D taskmanager.memory.fraction=0.001


write / generate

./bin/flink run -c com.dataartisans.persistence.KafkaSequenceWriter /home/robert/scratch/target/kafka-datagen-1.0-SNAPSHOT.jar 16 1 8 3 10k hdp22-w-1.c.astral-sorter-757.internal:6667,hdp22-w-0.c.astral-sorter-757.internal:6667,hdp22-m.c.astral-sorter-757.internal:6667 10000

read using console reader:
./bin/flink run -c com.dataartisans.persistence.KafkaSequenceWriter /home/robert/scratch/target/kafka-datagen-1.0-SNAPSHOT.jar 16 1 8 3 10k hdp22-w-1.c.astral-sorter-757.internal:6667,hdp22-w-0.c.astral-sorter-757.internal:6667,hdp22-m.c.astral-sorter-757.internal:6667 10000

read 
./bin/flink run -c com.dataartisans.persistence.KafkaConsumerTopology /home/robert/scratch/target/kafka-datagen-1.0-SNAPSHOT.jar ignore 8 8 1000 test hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181 smallest

offset checker
/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect hdp22-w-0.c.astral-sorter-757.internal:2181,hdp22-w-1.c.astral-sorter-757.internal:2181,hdp22-m.c.astral-sorter-757.internal:2181 --topic test --group flink-kafka-consumer-topology
