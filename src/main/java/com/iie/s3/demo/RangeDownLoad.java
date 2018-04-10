package com.iie.s3.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class RangeDownLoad {

    private static String downPath;
    private static String downPath2;
    private static String IP;
    private static String PORT;
    private static String BUCKET_NAME;
    private static String USER_NAME;
    private static String PASSWORD;
    private static String IF_MATCH;
    private File localFile;
    private long block;
    private static String LOCAL_PATH;
    private RandomAccessFile accessFile;

    public static void main(String[] args) {

        String THREAD_NUM = "10";
        String DOWN_FILE = "feng.jpg";
        downPath = "http://"+IP+":"+PORT+"/"+BUCKET_NAME+"/"+DOWN_FILE+"?method=get";
        downPath2 = "http://"+IP+":"+PORT+"/"+BUCKET_NAME+"/"+DOWN_FILE;
        LOCAL_PATH = "d:\\qqqqq\\feng.jpg";
        IP = "10.199.33.12";
        PORT = "8888";
        BUCKET_NAME = "s3_feng";
        USER_NAME = "dong";
        PASSWORD = "111111";
        IF_MATCH = "111";

        RangeDownLoad rangeDownLoad = new RangeDownLoad();

        Client client = Client.create();

        WebResource webResource = client.resource("http://10.199.33.12:8888/s3_feng/feng.jpg");

        ClientResponse response = webResource
                .header("User-Name", USER_NAME)
                .header("Password", PASSWORD)
                .header("If-Modified-Since", "Fri, 13 Nov 2015 14:47:53 GMT")
                .header("If-Unmodified-Since", "Fri, 13 Nov 2018 14:47:53 GMT")
                .header("If-Match", IF_MATCH)
                .header("If-None-Match", "abc")
                .accept("text/plain").method("HEAD", ClientResponse.class);

        String Content_Length = response.getHeaders().get("Content-Length").get(0);

        long dataLen = Integer.parseInt(Content_Length);
        response.close();
        System.out.println("dataLen: "+dataLen);
        if(0<dataLen&&dataLen<=10485760){

            rangeDownLoad.fileDownload(LOCAL_PATH);
        }else{
            RangeDownLoad threadDownload = new RangeDownLoad();
            try {
                int threadNum = Integer.parseInt(THREAD_NUM);
                threadDownload.rangeDownload(downPath, threadNum,dataLen);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void fileDownload(String downLoadPath){
        try {
            Client client1 = Client.create();
            WebResource webResource1 = client1.resource("http://10.199.33.12:8888/s3_feng/feng.jpg?method=get");
            ClientResponse response1 = webResource1
                    .header("User-Name", USER_NAME)
                    .header("Password", PASSWORD)
                    .accept("application/octet-stream").get(ClientResponse.class);

            System.out.println("Output from Server .... \n");
            System.out.println(response1.getStatus());
            System.out.println(response1.toString());

            InputStream in = response1.getEntityInputStream();
            OutputStream output = new FileOutputStream(downLoadPath);

            int len;
            byte[] aa = new byte[1024];
            while((len = in.read(aa))!=-1){
                output.write(aa,0,len);
            }
            in.close();
            output.close();
            response1.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void rangeDownload(String path, int threadCount, long dataLen) throws Exception {

        localFile = new File(LOCAL_PATH);
        accessFile = new RandomAccessFile(localFile, "rwd");
        accessFile.setLength(dataLen);
        accessFile.close();
        block = dataLen % threadCount == 0 ? dataLen / threadCount : dataLen / threadCount + 1;
        System.out.println("block: "+block);
        for (int i = 0; i < threadCount; i++) {
            new DownloadThread(i,dataLen).start();
        }
    }
    private final class DownloadThread extends Thread {
        private int threadid;
        private long startPosition;
        private long endPosition;
        public DownloadThread(int threadid,long dataLen) {
            this.threadid = threadid;
            startPosition = threadid * block;
            endPosition = (threadid + 1) * block - 1;
            if(dataLen<=(endPosition+1)){
                endPosition=dataLen-1;
            }
        }
        @Override
        public void run() {
            RandomAccessFile accessFile = null;
            localFile = new File(LOCAL_PATH);
            try {
                accessFile = new RandomAccessFile(localFile, "rwd");
                System.out.println("startPosition: "+threadid+": "+startPosition);
                System.out.println("endPosition: "+threadid+": "+endPosition);
                accessFile.seek(startPosition);
                Client client = Client.create();
                WebResource webResource = client.resource("http://10.199.33.12:8888/s3_feng/feng.jpg?method=get");
                ClientResponse response = webResource
                        .header("User-Name", USER_NAME)
                        .header("Password", PASSWORD)
                        .header("Range", "bytes=" + startPosition + "-" + endPosition)
                        .accept("application/octet-stream").get(ClientResponse.class);
                System.out.println("Output from Server .... \n");
                System.out.println(response.getStatus());
                System.out.println(response.toString());
                writeTo(accessFile, response);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private void writeTo(RandomAccessFile accessFile, ClientResponse response){
            InputStream is = null;
            try {
                int len = -1;
                is = response.getEntityInputStream();
                byte[] buffer = new byte[1024*1024*2];
                while ((len = is.read(buffer)) != -1) {
                    accessFile.write(buffer, 0, len);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if(is != null) {
                        is.close();
                    }
                    if(accessFile != null) {
                        accessFile.close();
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                response.close();
            }
        }
    }
}
