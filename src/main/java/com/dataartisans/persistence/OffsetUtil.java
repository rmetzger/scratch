package com.dataartisans.persistence;

import kafka.consumer.ConsumerConfig;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;

import java.util.Properties;

/**
 * Created by robert on 5/6/15.
 */
public class OffsetUtil {

	public static void main(String[] args) {

		Properties cProps = new Properties();
		cProps.setProperty("zookeeper.connect", args[1]);
		cProps.setProperty("group.id", args[2]);
		cProps.setProperty("auto.commit.enable", "false");

		cProps.setProperty("auto.offset.reset", "smallest"); // read from the beginning.

		ConsumerConfig standardCC = new ConsumerConfig(cProps);

		ZkClient zk = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new PersistentKafkaSource.KafkaZKStringSerializer());
		if(args[0].equals("get")) {
			System.out.println("Partition = " + PersistentKafkaSource.getOffset(zk, standardCC.groupId(), args[3] /*topic*/, Integer.valueOf(args[4]) /*partition*/));
		} else if(args[0].equals("set")) {
			PersistentKafkaSource.setOffset(zk, standardCC.groupId(), args[3] /*topic*/, Integer.valueOf(args[4]) /*partition*/, Long.valueOf(args[5]));
		}
	}
}
