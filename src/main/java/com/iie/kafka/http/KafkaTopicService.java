package com.iie.kafka.http;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import org.apache.avro.Schema;
import org.apache.kafka.common.security.JaasUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;


public class KafkaTopicService {
    private final static Logger logger = LoggerFactory.getLogger(KafkaTopicService.class);
    public CachedSchemaRegistryClient cachedSchemaRegistryClient;

    public static void main(String[] args) throws Exception {
        String schemaRegistryServer = "http://10.199.33.13:8081";
        String zkConnect = "10.199.33.14:2181/kafka";
        String topicName = "test8";
        int partitions = 1;
        int replicationFactor = 2;
        String delteTime = "30000";

        KafkaTopicService kafka = new KafkaTopicService();
//        Boolean flag = kafka.createTopic(zkConnect,topicName,partitions,replicationFactor,delteTime);
//        System.out.println("==============="+flag);

        //注册schema
        String topicSchemaJson = "{\"type\":\"record\",\"name\":\"t_dams_ab_dname\",\"fields\":[{\"name\":\"C_TIME\",\"type\":\"string\"},{\"name\":\"C_PCODE\",\"type\":\"string\"},{\"name\":\"C_NODEIP\",\"type\":\"long\"},{\"name\":\"C_PROTOCOL\",\"type\":\"int\"},{\"name\":\"C_SIP\",\"type\":\"long\"},{\"name\":\"C_DIP\",\"type\":\"long\"},{\"name\":\"C_DOMAIN\",\"type\":\"string\"}]}";
        System.out.println("==============="+kafka.registrySchema(schemaRegistryServer,topicName,topicSchemaJson));

        //获取schema
        String schema = kafka.getTopicSchemaJson(topicName,schemaRegistryServer);

        //Boolean flag = kafka.deleteTopic(zkConnect,topicName);
       // System.out.println("==============="+flag);

    }

    public Boolean createTopic( String zkConnect,String topicName,int partitions,int replicationFactor,String deleteTime){
        ZkUtils zkUtils = ZkUtils.apply(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = new Properties();
        props.put("delete.retention.ms", deleteTime);
        props.put("retention.ms",deleteTime);
        if(!AdminUtils.topicExists(zkUtils,topicName)){
            AdminUtils.createTopic(zkUtils,topicName, partitions, replicationFactor, props, RackAwareMode.Enforced$.MODULE$);
            zkUtils.close();
            logger.info("Topic "+topicName+" create success!");
            return true;
        }else {
            logger.info("Topic {} already exists", topicName);
        }
        zkUtils.close();
        return false;
    }

    /**
     * 读取schema
     * @param topic
     * @param schemaRegistryServer
     * @return
     * @throws IOException
     * @throws RestClientException
     */
    public String getTopicSchemaJson(String topic,String schemaRegistryServer)throws IOException, RestClientException {

        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryServer, 100);
        Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());
        String srcTopicSchemajson = schema.toString();
        System.out.println("topic "+topic+"'schema is"+srcTopicSchemajson);
        return srcTopicSchemajson;
    }

    public Boolean registrySchema(String schemaRegistryServer,String topicName,String topicSchemaJson)throws  Exception{
        JSONObject schemaObj = new JSONObject(topicSchemaJson);
        Schema filterAvroSchema = new Schema.Parser().parse(schemaObj.toString());
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryServer, 100);
        int registryHttpStatusCode = cachedSchemaRegistryClient.register(topicName, filterAvroSchema);
        try{
            if(registryHttpStatusCode == 200){
                logger.info("===========" + topicName+"registry success!");
                System.out.println("===========" + topicName+"registry success!");
                return true;
            }
        }catch (Exception e){
            logger.error("===========" + topicName+"registry failed!");
            System.out.println("===========" + topicName+"registry failed!"+e.getMessage());
            return false;
        }
        return false;
    }

    public Boolean deleteTopic( String zkConnect,String topicName){
        ZkUtils zkUtils = ZkUtils.apply(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 删除topic
        if(AdminUtils.topicExists(zkUtils,topicName)){
            AdminUtils.deleteTopic(zkUtils, topicName);
            zkUtils.close();
            logger.info("Topic "+topicName+" delete success!");
            return true;
        }else {
            logger.info("Topic {} is not exist", topicName);
        }
        return false;
    }
}