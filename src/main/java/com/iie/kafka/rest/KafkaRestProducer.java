package com.iie.kafka.rest;

import com.hubrick.kafka.confluent.core.ClientFactory;
import com.hubrick.kafka.confluent.producer.ConfluentKafkaProducer;
import com.hubrick.kafka.confluent.producer.ProducerRecord;

import javax.ws.rs.client.ClientBuilder;


/**
 * Created by x on 10/9/16.
 */
public class KafkaRestProducer {

	public static void main(String[] args) {

		final ConfluentKafkaProducer producer =
				new ConfluentKafkaProducer("http://10.199.33.13:8082", ClientBuilder::newClient);

//		final KeyedMessage<String, String> message =
//				KeyedMessage.asKeyedMessage("test11", "Here we go"); // specify topic and message
//
//		producer.send(message);

		final ProducerRecord record = new ProducerRecord();
		record.setKey("c67f056b-2349-4367-8d5d-c47311fc713c".getBytes());
		record.setValue("Hello there!".getBytes());
		record.setPartition(0);

		producer.send("test13", record);
	}
}
