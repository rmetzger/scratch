//package com.dataartisans;
//
//import org.apache.activemq.ActiveMQConnectionFactory;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.activemq.AMQSource;
//import org.apache.flink.streaming.connectors.activemq.AMQSourceConfig;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//
//public class Consumer {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        AMQSourceConfig.AMQSourceConfigBuilder<String> config = new AMQSourceConfig.AMQSourceConfigBuilder<>();
//        config.setDestinationName("test");
//        config.setDeserializationSchema(new SimpleStringSchema());
//        config.setConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:61616?trace=false&soTimeout=60000"));
//
//        DataStream<String> src = see.addSource(new AMQSource<>(config.build()));
//        src.print();
//
//        see.execute("aha");
//    }
//}
