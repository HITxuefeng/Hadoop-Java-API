package com.iie.es.demo;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import org.elasticsearch.client.transport.TransportClient;

import java.net.UnknownHostException;
import java.util.Date;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class JavaESTest {
    public static void main(String args[]){
        JavaESTest test = new JavaESTest();
        Settings settings = Settings.builder().put("cluster.name", "es-test").put("transport.type","netty3")
                .put("http.type", "netty3")
                .put("client.transport.sniff", true).build();
        try {
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.199.33.11"), 9300));
            test.prepareIndex(client);
            test.prepareGet(client);
            //test.prepareDelete(client);
            client.close();
        } catch (UnknownHostException e) {
           e.printStackTrace();
        }
    }

    //对ES进行增的操作
    public void prepareIndex(TransportClient client){
        try {
            IndexResponse response = client.prepareIndex("twitter", "tweet", "2")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject()
                    ).get();
            System.out.println(response.status().getStatus());
        }
        catch(Exception e)
        {
            //
        }
    }

    //对ES进行查的操作
    public void prepareGet(TransportClient client){
        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
        String strTmp2 = response.toString();
        System.out.println(strTmp2);
    }

    //删除的操作
    public void prepareDelete(TransportClient client){
        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();
        String strTmp2 = response.toString();
        System.out.println(strTmp2);
    }
}