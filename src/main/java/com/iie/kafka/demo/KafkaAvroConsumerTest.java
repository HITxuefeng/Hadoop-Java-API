package com.iie.kafka.demo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class KafkaAvroConsumerTest {
	public static void main(String[] args) {
//      String topic=args[0];
//      String group=args[1];
//		String url=args[2];
		int count=0;

		String topic="hr_jiangan_test";
		String group="fffjx123";
		String url="node2a12:9092,node2a13:9092,node2a14:9092";

        Properties props =new Properties();
        props.put("max.partition.fetch.bytes", 10485760);
        props.put("auto.offset.reset", "earliest");
//		props.put("auto.offset.reset", "latest");
		props.put("bootstrap.servers", url);
        props.put("group.id",group);
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        //获取schema
		try {
			Schema.Parser parser = new Schema.Parser();
			//专网schema地址
			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://88.1.2.6:8081,http://88.1.2.7:8081", 100);
			//互联网schema地址
//			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://192.168.233.4:8081,http://192.168.233.5:8081", 100);
			Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
			System.out.println("*************** schema ***************");
			System.out.println(schema.toString());
			System.out.println("**************************************");
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			//获取topic
			consumer.subscribe(Arrays.asList(topic));
			GenericRecord grecord = new GenericData.Record(schema);
			//开始消费
			while(true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(10);
				for(ConsumerRecord<String, byte[]> record : records) {
					//对每个record进行反序列化
					count++;
					byte[] bytes=record.value();
					BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
					//对数据的业务操作实现在这里
					while (!binaryDecoder.isEnd()) {
						System.out.println("partiton:-------"+record.partition()+"---------offset"+record.offset());
						reader.read(grecord, binaryDecoder);
						System.out.println(grecord+"-------------"+count);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
 	}
}
