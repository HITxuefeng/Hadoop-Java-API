package com.iie.kafka.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class KafkaRestPostBinaryDemo {

	public static void main(String[] args) throws Exception {

		DefaultHttpClient httpClient = new DefaultHttpClient();
		String topic = "http-test11";

		try {
			HttpPost request = new HttpPost("http://10.199.33.13:8082/topics/"+topic);

			String str = "{\"records\":[{\"value\":\"S2Fma2E=\"}]}";
			request.addHeader("Content-Type","application/vnd.kafka.binary.v1+json;charset=UTF-8");

			//HttpEntity httpEntity = new StringEntity(str);
			HttpEntity httpEntity = new ByteArrayEntity(str.getBytes());
			request.setEntity(httpEntity);
			//do not skip it!!!!!
			HttpResponse response = httpClient.execute(request);
			System.out.println("send message: "+EntityUtils.toString(httpEntity));
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


