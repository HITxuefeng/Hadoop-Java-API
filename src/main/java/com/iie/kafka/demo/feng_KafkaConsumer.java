package com.iie.kafka.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class feng_KafkaConsumer {
    public static void main(String[] args) {
//        String topic=args[0];
//        String group=args[1];
//        String url=args[2];
        String topic="test_002";
        String group="group201812qw1233";
        String url="kvdb05:9095";

        Properties props =new Properties();
        props.put("max.partition.fetch.bytes", 10485760);
//        props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "latest");
        props.put("bootstrap.servers", url);
        props.put("group.id",group);
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

//        System.out.println("*******使配置项生效***********");
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer(group,url);

//        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(props);
        //获取schema
        //            Schema.Parser parser = new Schema.Parser();
//            CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.13:8081", 100);
//            Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
//
//            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        //获取topic
        consumer.subscribe(Arrays.asList(topic));
//            GenericRecord grecord = new GenericData.Record(schema);

//        System.out.println("----------获取topic---------");

        consumer.subscribe(Arrays.asList(topic));
        //开始消费
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(10);

//            System.out.println("888888888888888888");
            for(ConsumerRecord<String, String> record : records) {
//                byte[] bytes=record.value();
//                BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);

                System.out.println(record.value().toString());
//                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
        }
    }
}
