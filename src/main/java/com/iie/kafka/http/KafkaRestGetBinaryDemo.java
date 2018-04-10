package com.iie.kafka.http;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class KafkaRestGetBinaryDemo {

	public static void main(String[] args) throws Exception {

		DefaultHttpClient httpClient = new DefaultHttpClient();

		String topic = "fang-test10";
		String groupID = "consumer_50";

		try {
			//KafkaRestConsumerInstance consumerInstance = new KafkaRestConsumerInstance();
			//String base_uri = consumerInstance.getConsumerInstance(groupID);
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

			String base_uri="http://10.199.33.13:8082/consumers/consumera5/instances/rest-consumer-kafka-rest-test-server-7c0f7f88-26e9-4700-80ff-370bbdde2b21";
			HttpGet request = new HttpGet(base_uri+"/topics/"+topic);
			request.addHeader("Accept","application/vnd.kafka.binary.v1+json;charset=UTF-8");
			HttpResponse response = httpClient.execute(request);
			System.out.println("http return status: " + response.getStatusLine().getStatusCode());
			HttpEntity httpEntity = response.getEntity();

//			byte[] source =readStream(httpEntity.getContent());
//			byte[] str = Base64.decodeBase64(source);
			String str = EntityUtils.toString(httpEntity);
			System.out.println("-------------------"+str);
			String str3 = str.substring(1, str.length()-1);
			//System.out.println("-------------------str3 : "+str3);
			JSONObject json2 = new JSONObject(str3);
			//System.out.println("value------" + json2.get("value"));
			java.util.Base64.Decoder decoder = java.util.Base64.getDecoder();

			String encodeText2 = json2.getString("value");
			System.out.println("value:" + encodeText2);
			byte[] bytes  = decoder.decode(encodeText2);
			System.out.println("bytes.len:" + bytes.length);
			System.out.println("bytes:" + new String(bytes, "UTF-8"));
			//String re = new String(decoder.decode((byte[]) json2.get("value")), "UTF-8");
			//System.out.println("-------------------re : "+re);

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

			//String str =EntityUtils.toString(httpEntity);
			//System.out.println("-------------------"+httpEntity.toString());

//			JSONObject json = new JSONObject(str);
//			System.out.println("-------------------"+json.get("value"));
			} catch (Exception ex) {
				// handle response here... try other servers
			} finally {
				httpClient.getConnectionManager().shutdown(); //Deprecated
			}
		if (httpClient != null) {
			httpClient.getConnectionManager().shutdown(); //Deprecated
		}
	}

}


