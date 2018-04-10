package com.iie.kafka.http;
import java.io.*;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * Created by wei on 9/19/2016.
 */
public class HttpProducer {

    public static void main(String[] args) throws Exception {

        DefaultHttpClient httpClient = new DefaultHttpClient();

        // get http server list
        int curHTTPServer = 0;

        String[] ips = new String[]{"http://10.199.33.12:10080","http://10.199.33.13:10080","http://10.199.33.14:10080"};
//		String[] ips = new String[]{"http://172.16.240.2:10081","http://172.16.240.2:10082","http://172.16.240.2:10083"};

        // get schema
        String topic = "xuefeng";

        /**
         * 从http://SchemaRegistryServer:8081/subjects/{topic}获取schema，该部分需要开发者自己实现
         * 该URL的返回结果如下：
         * {"subject":"test-ha","version":1,"id":81,"schema":"{\"type\":\"record\",\"name\":\"t_dams_ab_dname\",\"fields\":[{\"name\":\"C_TIME\",\"type\":\"string\"},{\"name\":\"C_PCODE\",\"type\":\"string\"},{\"name\":\"C_NODEIP\",\"type\":\"long\"},{\"name\":\"C_PROTOCOL\",\"type\":\"int\"},{\"name\":\"C_SIP\",\"type\":\"long\"},{\"name\":\"C_DIP\",\"type\":\"long\"},{\"name\":\"C_DOMAIN\",\"type\":\"string\"}]}"}
         *
         * */
        /**
         * 该处为使用schema-registry客户端获取schema，并按照上面给出的格式说明解析出schema
         */
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.13:8081", 100);
        //CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://172.16.240.2:8086", 100);
        Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());

        System.out.println("*************** schema ***************");
        System.out.println(schema.toString());
        System.out.println("**************************************");

        //数据序列化
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        // ~=10MB
        ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        //for (int i = 0; i < 1; i++) {
        // 构造一批数据，推荐大小为5MB左右
        out.reset();
        for (int j = 0; j < 1; j++) {
            GenericRecord record = new GenericData.Record(schema);
            System.out.println("field size: " + schema.getFields().size());

//            record.put("name","雪峰123");
//            record.put("fromId", "汉字555");
//            record.put("checkResult", "20170808");
//            record.put("orgCode","111");
//            record.put("reply", "20170808");
//				record.put("C_DIP", (long)20170808);
//				record.put("C_DOMAIN", "201708098");

            record.put("transCode","xuefeng");
            record.put("IdCardNo", "20170808");
            record.put("StudyInstanceUID", "555555555555");
            record.put("SopInstanceUID", "333333333333");
            System.out.println("record:" + record);
            writer.write(record, encoder);
        }
        // send to http server
        encoder.flush();
        out.flush();
        System.out.println("bytes.len:" + out.toByteArray().length);
        System.out.println("bytes.len:" + out.toByteArray());
        try {
            // load balance
            HttpPost request = new HttpPost(ips[curHTTPServer]);
            curHTTPServer++;
            curHTTPServer %= ips.length;


            // set header
            request.addHeader("content-type", "utf-8");
//            request.addHeader("User", "LiMing");
//            request.addHeader("Password", "123");
            request.addHeader("Topic", topic);
            request.addHeader("Format", "avro");
            HttpEntity httpEntity = new ByteArrayEntity(out.toByteArray());
            request.setEntity(httpEntity);
            //do not skip it!!!!!
            HttpResponse response = httpClient.execute(request);
            System.out.println("http return status: " + response.getStatusLine().getStatusCode());
        } catch (Exception ex) {
            // handle response here... try other servers
        } finally {
            httpClient.getConnectionManager().shutdown(); //Deprecated
        }
        //}
        if (httpClient != null) {
            httpClient.getConnectionManager().shutdown(); //Deprecated
        }
    }

}


