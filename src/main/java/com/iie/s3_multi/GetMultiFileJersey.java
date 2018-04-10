package com.iie.s3_multi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class GetMultiFileJersey implements Runnable{

    int id;
    //	String uploadFilePath = "/home/zpb/testfile";
    String downloadFilePath = "D:\\qqqqq\\";
    String ip = "10.199.33.12";
    String port = "8888";
    String bucketName = "s3_feng";
    String userName ="dong" ;
    String passwd ="111111" ;
    String objNames = "feng.jpg,bbb.jpg";


    final Client client = Client.create();
    byte[] bt = new byte[8192];
    FileInputStream in;


    public static void main(String[] args) {

        int num = 1;//Integer.parseInt(args[0]);
        MultiThread(num);
    }

    public static void MultiThread(int num){
        for (int i = 0; i < num; i++) {
            Thread iThread =new Thread(new GetMultiFileJersey(i));
            iThread.start();
        }
    }


    public GetMultiFileJersey(int id){
        this.id=id;
    }

    @Override
    public void run() {

        try {
            WebResource webResource = client.resource("http://" + ip + ":"+port+"/" + bucketName+"/multiGet");
            for(int i =0;i<1;i++){
                ClientResponse response = webResource
                        .header("User-Name", userName)
                        .header("Password", passwd)
                        .header("ObjNames", objNames)
                        .type("application/octet-stream")
                        .get(ClientResponse.class);

                InputStream in = response.getEntityInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ByteArray2int byteArray2Int = new ByteArray2int();
                HashMap<String,byte[]> map = new HashMap<String, byte[]>();
                int len;
                byte[] ba = new byte[1024];
                while((len=in.read(ba))!=-1){
                    out.write(ba,0,len);
                }
                in.close();

                byte[] body = out.toByteArray();
                int totalen = 0;
                while(totalen<body.length){
                    byte[] byte_1 = new byte[4];
                    System.arraycopy(body, totalen, byte_1, 0, 4);
                    totalen+=4;
                    int keyLen1 = byteArray2Int.ba2i(byte_1);
                    byte[] byte_2 = new byte[keyLen1];
                    System.arraycopy(body, totalen, byte_2, 0, keyLen1);
                    String fileName = new String(byte_2);
                    totalen+=keyLen1;
                    byte[] byte_3 = new byte[4];
                    System.arraycopy(body, totalen, byte_3, 0, 4);
                    totalen+=4;
                    int keyLen2 = byteArray2Int.ba2i(byte_3);
                    byte[] byte_4 = new byte[keyLen2];
                    System.arraycopy(body, totalen, byte_4, 0, keyLen2);
                    totalen+=keyLen2;
                    map.put(fileName, byte_4);
                }

                Set<Entry<String, byte[]>> set = map.entrySet();
                Iterator<Entry<String, byte[]>> items = set.iterator();

                while(items.hasNext()){

                    Entry<String, byte[]>  entry = items.next();
                    String key = entry.getKey();
                    byte[] value = entry.getValue();

                    InputStream input =new ByteArrayInputStream(value);
                    OutputStream output = new FileOutputStream(downloadFilePath+key);

                    int length;
                    byte[] aa = new byte[1024];
                    while((len = input.read(aa))!=-1){
                        output.write(aa,0,len);
                    }
                    input.close();
                    output.close();
                }
                response.close();
            }
            client.destroy();

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}
