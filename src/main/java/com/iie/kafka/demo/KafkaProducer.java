package com.iie.kafka.demo;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class KafkaProducer {
    public static void main(String[] args) throws Exception{
//        String topic=args[0];
//        String url=args[1];

        String topic="test_002";
        String url="kvdb05:9095";
//        String topic = "test_002";
//        String brokerList = "10.199.33.11:9092,10.199.33.13:9092,10.199.33.14:9092";
//        String brokerList = "10.199.33.11:9092";
        String brokerList=url;
        Producer<String, String> producer = KafkaUtil.getProducer(brokerList);
        String i = "----------5555";
       //while(true) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, String.valueOf(i), "this is message"+i);
           System.out.println("*******Send success!");
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());
                }
            });
//            i++;
            Thread.sleep(100);
       // }
    }
}