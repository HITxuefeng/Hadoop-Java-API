package com.iie.kafka.rest;

import com.hubrick.kafka.confluent.consumer.ConfluentKafkaConsumer;
import com.hubrick.kafka.confluent.consumer.ConsumerConfig;
import com.hubrick.kafka.confluent.consumer.ConsumerRecord;
import com.hubrick.kafka.confluent.core.ClientFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import javax.ws.rs.client.ClientBuilder;
import java.io.IOException;
import java.util.Collection;

/**
 * Created by x on 10/9/16.
 */
public class KafkaRestAvroConsumer {

	public static void main(String[] args) {

		String topic = "http-test1";
		String group = "consumer_198";

		final ConsumerConfig config = new ConsumerConfig(group);
		config.setAutoOffsetReset("largest");
		//config.setAutoOffsetReset("earliest");
		config.setAutoCommitEnable(true);
		config.setTimeout(5000L);

		final ConfluentKafkaConsumer consumer =
				new ConfluentKafkaConsumer("http://10.199.33.13:8082", config, ClientBuilder::newClient);

		consumer.start(); // consumer must be started before it can be used

		final Collection<ConsumerRecord> records = consumer.fetch(topic);

		//System.out.println(records.isEmpty());
		Schema.Parser parser = new Schema.Parser();
		CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.13:8081", 100);
		Schema schema = null;
		try {
			schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}

		System.out.println("*************** schema ***************");
		System.out.println(schema.toString());
		System.out.println("**************************************");

		for(ConsumerRecord record : records) {
			//对每个record进行反序列化
			//System.out.println(record.getKey().toString()+":"+record.getValue().toString()+":"+record.getOffset()+":"+record.getPartition());
			byte[] bytes = record.getValue();
			//System.out.println("bytes.len:" + bytes.length);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			//获取topic
			GenericRecord grecord = new GenericData.Record(schema);

			BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);

			//对数据的业务操作实现在这里
			try {
				while (!binaryDecoder.isEnd()) {
					reader.read(grecord, binaryDecoder);
					System.out.println(grecord);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		consumer.shutdown();
	}
}
