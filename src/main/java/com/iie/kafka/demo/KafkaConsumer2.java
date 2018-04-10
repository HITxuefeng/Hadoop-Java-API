package com.iie.kafka.demo;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer2 {

	public static void main(String[] args) {

        String topic = "test_002";

        Properties prop = new Properties();
        prop.put("zookeeper.connect", "kvdb05:2183/kafka3");
//        prop.put("metadata.broker.list", "10.199.33.11:9092");

//        prop.put("bootstrap.servers","10.199.33.11:9092");
        prop.put("group.id", "group20181211122");
        prop.put("auto.offset.reset", "smallest");
        //prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        final KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
        while (iterator.hasNext()) {
//            String msg = new String(iterator.next().message());
//            System.out.println("--------"+msg);
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                System.out.println("partiton:" + next.partition() +" offset:" + next.offset() + "--------"+new String(next.message()));
        }
	}
}
