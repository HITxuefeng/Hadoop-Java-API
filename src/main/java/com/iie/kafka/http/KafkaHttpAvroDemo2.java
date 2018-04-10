package com.iie.kafka.http;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * Created by xuefeng on 9/10/2017.
 */
public class KafkaHttpAvroDemo2 {

	public static void main(String[] args) throws Exception {

		DefaultHttpClient httpClient = new DefaultHttpClient();

		// get http server list
		int curHTTPServer = 0;
		String[] ips = new String[]{"http://10.199.33.12:10080","http://10.199.33.13:10080","http://10.199.33.14:10080"};

		// get schema
		String topic = "feng-testa2";

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
		CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://10.199.33.14:8081,http://10.199.33.13:8081", 100);
		Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
		Schema RefengSchema=Schema.parse("{\"type\":\"record\",\"name\":\"Refeng1\",\"fields\":[{\"name\":\"feng11\",\"type\":\"string\"},{\"name\":\"feng12\",\"type\":\"string\"}]}");
		Schema RefengSchema1=Schema.parse("{\"type\":\"record\",\"name\":\"Refeng2\",\"fields\":[{\"name\":\"feng21\",\"type\":\"string\"},{\"name\":\"feng22\",\"type\":\"string\"},{\"name\":\"Re2\",\"type\":{\"type\":\"record\",\"name\":\"Refeng3\",\"fields\":[{\"name\":\"feng21\",\"type\":\"string\"},{\"name\":\"feng22\",\"type\":\"string\"}]}}]}");
		Schema RefengSchema2=Schema.parse("{\"type\":\"record\",\"name\":\"Refeng3\",\"fields\":[{\"name\":\"feng21\",\"type\":\"string\"},{\"name\":\"feng22\",\"type\":\"string\"}]}");


		System.out.println("*************** schema ***************");
		System.out.println(schema.toString());
		System.out.println("**************************************");

		/**
		 * 如果shcema fields中嵌套了record，根据field name获取镶嵌record的schema
		 */
//		Schema schema2 = schema.getField("Re1").schema();
//		Schema schema5 = schema.getField("tags").schema();
//		Schema schema3 = schema2.getField("Re2").schema();
//		Schema schema3 = schema5.getTypes().get(1);

//		Schema schema3 = schema.getField("Refeng1").schema();

//		System.out.println(schema5.toString());
//		System.out.println(schema2.toString());

		//数据序列化
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		// ~=10MB
		ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		for (int i = 0; i < 1; i++) {
			// 构造一批数据，推荐大小为5MB左右
			out.reset();
			for (int j = 0; j < 20; j++) {
				GenericRecord record = new GenericData.Record(schema);

                //定义array类型数据
				List list =new LinkedList<String> ();
//				list.add("col1");
//				list.add("col2");

				List list1=new LinkedList<Object>();

				GenericData.Record Refeng11=new GenericData.Record(RefengSchema);
				Refeng11.put("feng11","dongfang11");
				Refeng11.put("feng12","xuefeng12");
				list1.add(Refeng11);

                //定义map类型数据
				HashMap map=new HashMap();
				map.put("a","aaaaa");
				map.put("b","11111");

				GenericData.Record Refeng33=new GenericData.Record(RefengSchema2);
				Refeng33.put("feng21","xuefeng21");
				Refeng33.put("feng22","xuefeng22");

				//定义嵌入的record类型数据，schema2为此record的schema
				GenericData.Record Refeng22=new GenericData.Record(RefengSchema1);
				Refeng22.put("feng21","xuefeng21");
				Refeng22.put("feng22","xuefeng22");
				Refeng22.put("Re2",Refeng33);

                //定义其他普通类型数据
				record.put ("C_TIME","feng");
				record.put ("C_NODEIP",11l);
				record.put ("C_PROTOCOL",22);
				record.put("tags",list1);  //array
				record.put("sign",map);   //map
				record.put("Re1",Refeng22);//record


				writer.write(record, encoder);
			}
			// send to http server
			encoder.flush();
			out.flush();
			try {
				// load balance
				HttpPost request = new HttpPost(ips[curHTTPServer]);
				curHTTPServer++;
				curHTTPServer %= ips.length;

				// set header
				request.addHeader("content-type", "utf-8");
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
		}
		if (httpClient != null) {
			httpClient.getConnectionManager().shutdown(); //Deprecated
		}
	}
}


