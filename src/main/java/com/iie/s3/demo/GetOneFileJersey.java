package com.iie.s3.demo;

import java.io.*;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class  GetOneFileJersey implements Runnable{

    int id;
    static String userName;
    static String passwd;
    static String uploadFileName;
    static String downloadFileName;

    static String uploadFilePath;
    static String downloadFilePath;
    static String BUCKETNAME;
    static String OBJECTNAME;
    static int num;
    static int fornum;//循环次数

    final Client client = Client.create();
    byte[] bt = new byte[8192];   //8K

    FileInputStream in;


    public static void main(String[] args) throws InterruptedException {
        userName ="wuhan" ;
        passwd ="111111" ;
//        uploadFileName="ditu.jpg";
//        downloadFileName="ditu.jpg";
//        uploadFilePath = "d:\\xxxxx\\"+uploadFileName;
//        downloadFilePath = "d:\\xxxxx\\"+downloadFileName;
//        BUCKETNAME = "s3_feng";
//        OBJECTNAME = "ff.jpg";
        num = 1;
        fornum=1;
        MultiThread();
    }

    public static synchronized void  MultiThread() throws InterruptedException{
        for (int i = 0; i < num; i++) {
            Thread iThread =new Thread(new GetOneFileJersey(i));
            iThread.start();
        }
    }


    public GetOneFileJersey(int id){
        this.id=id;
    }

    @Override
    public void run() {


        Client client = Client.create();

        WebResource webResource1 = client.resource("http://88.1.2.15:8888/s3_test/feng.jpg");

        ClientResponse response1 = webResource1
                .header("User-Name", userName)
                .header("Password", passwd)
                .header("If-Modified-Since", "Fri, 13 Nov 2015 14:47:53 GMT")
                .header("If-Unmodified-Since", "Fri, 13 Nov 2018 14:47:53 GMT")
                .header("If-Match", "111")
                .header("If-None-Match", "abc")
                .accept("text/plain").method("HEAD", ClientResponse.class);

        String Content_Length = response1.getHeaders().get("Content-Length").get(0);

        long dataLen = Integer.parseInt(Content_Length);
        response1.close();
        System.out.println("dataLen: "+dataLen);


        try {
            WebResource webResource = client.resource("http://88.1.2.15:8888/s3_test/feng.jpg?method=get");
            ClientResponse response = webResource
                    .header("User-Name", userName)
                    .header("Password", passwd)
                    .header("If-Modified-Since", "2011-12-01 12:27:13.000 GMT")
                    .header("If-Unmodified-Since", "2040-12-01 12:27:13.000 GMT")
                    .header("If-Match", "abc")
                    .header("If-None-Match", "abc")
                    .header("Range", "bytes=" + 0 + "-" + (dataLen-1))
                    .accept("application/oct-stream").get(ClientResponse.class);
            InputStream is = null;
            is = response.getEntityInputStream();
            File file=new File("d:\\qqqqq\\9999.jpg");
            FileOutputStream fos=new FileOutputStream(file);


            byte[] buffer = new byte[1024*1024*40];
            int len = -1;
            while ((len = is.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
            fos.flush();
            fos.close();
            response.getStatus();
            System.out.println(response.getStatus());//输出为200时说明程序成功执行
            response.close();
            client.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
