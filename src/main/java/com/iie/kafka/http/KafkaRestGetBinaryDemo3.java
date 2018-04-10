package com.iie.kafka.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.util.Base64;

public class KafkaRestGetBinaryDemo3 {

	public static void main(String[] args) throws Exception {

		DefaultHttpClient httpClient = new DefaultHttpClient();

		String topic = "http-test1";
		String groupID = "consumer_50";

		try {
			//KafkaRestConsumerInstance consumerInstance = new KafkaRestConsumerInstance();
			//String base_uri = consumerInstance.getConsumerInstance(groupID);
			String base_uri="http://10.199.33.13:8082/consumers/consumer_51/instances/rest-consumer-kafka-rest-test-server-a4e54b2d-8d6f-4235-8f49-2404fca1ca13";
			HttpGet request = new HttpGet(base_uri+"/topics/"+topic);

			request.addHeader("Accept","application/vnd.kafka.binary.v1+json;charset=UTF-8");

			HttpResponse response = httpClient.execute(request);
			System.out.println("http return status: " + response.getStatusLine().getStatusCode());
			HttpEntity httpEntity = response.getEntity();
			//httpEntity.writeTo();
			String str = EntityUtils.toString(httpEntity);
			System.out.println("-------------------"+str);
			String str3 = str.substring(1, str.length()-1);
//			Base64.Decoder decoder = Base64.getDecoder();
//			System.out.println("============="+new String(decoder.decode(str), "UTF-8"));

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


