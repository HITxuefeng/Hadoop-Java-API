package com.iie.kafka.http;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class Consumer_Test {
    public static void main(String[] args) {

//        String topic="hr_jiangan";

//        String group="group1111111111123";
//        String group="feng1234";

  		String topic = args[0];
		String group = args[1];

        Properties props =new Properties();
        props.put("max.partition.fetch.bytes", 10485760);
//        props.put("auto.offset.reset", "smallest");
        props.put("auto.offset.reset", "largest");
        props.put("zookeeper.connect", "10.199.33.14:2181/kafka");
        props.put("group.id",group);
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        //KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        //获取schema
        try {
            Schema.Parser parser = new Schema.Parser();
            CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.13:8081", 100);
//			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://172.16.240.2:8086", 100);
            Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());

            System.out.println("*************** schema ***************");
            System.out.println(schema.toString());
            System.out.println("**************************************");

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            //获取topic
            //consumer.subscribe(Arrays.asList(topic));
            GenericRecord grecord = new GenericData.Record(schema);
            ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
            final KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topic).get(0);
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();

            //开始消费

            while (iterator.hasNext()) {
                //对每个record进行反序列化
                byte[] bytes = iterator.next().message();
                BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);


                //对数据的业务操作实现在这里
                while (!binaryDecoder.isEnd()) {
                    try {

                        reader.read(grecord, binaryDecoder);
//                        System.out.println(grecord);


                        Date now = new Date();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//可以方便地修改日期格式

                        byte[] buff=new byte[]{};
                        String hehe = dateFormat.format( now );
                        File f = new File("/home/feng/yuanshengconsumer2_"+topic+".log");
                        OutputStream os = new FileOutputStream(f,true);
                        String buffer=grecord.toString();
                        String buffer1=hehe+buffer;
                        buffer1+="\r\n";
                        buff=buffer1.getBytes();
                        os.write(buff);
                        os.flush();
                        os.close();


                    }catch (IOException e){
                        e.printStackTrace();
                        continue;
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

