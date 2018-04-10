package com.iie.s3.demo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class FileDownLoad {

    private static String IP;
    private static String PORT;
    private static String USER_NAME;
    private static String PASSWORD;
    private static String DOWNLOADPATH;
    private static String LOCAL_PATH;
    private static String BUCKET_NAME;
    private static String DOWNLOADPATH_META;
    private static String DOWNLOADFILENAME;
    private static long range = 10485759*10;//100M

    private Client client = Client.create();

    public long getFileLength(String downLoadFileName){

        DOWNLOADPATH_META = "http://"+IP+":"+PORT+"/"+BUCKET_NAME+"/"+downLoadFileName;


        Client client = Client.create();

        WebResource webResource1 = client.resource(DOWNLOADPATH_META);

        ClientResponse response1 = webResource1
                .header("User-Name", "dong")
                .header("Password", "111111")
                .header("If-Modified-Since", "Fri, 13 Nov 2015 14:47:53 GMT")
                .header("If-Unmodified-Since", "Fri, 13 Nov 2018 14:47:53 GMT")
                .header("If-Match", "111")
                .header("If-None-Match", "abc")
                .accept("text/plain").method("HEAD", ClientResponse.class);

        String Content_Length = response1.getHeaders().get("Content-Length").get(0);

        long fileLength = Integer.parseInt(Content_Length);
        response1.close();
        System.out.println("dataLen: "+fileLength);
        return fileLength;
    }

    public boolean isBigFile(long fileLength){
        boolean isMore = false;
        if(fileLength>10485760){
            isMore = true;
        }
        return isMore;
    }

    public void smallFileDownLoad(){
        try {
            WebResource webResource = client.resource(DOWNLOADPATH);
            ClientResponse response = webResource
                    .header("User-Name", USER_NAME)
                    .header("Password", PASSWORD)
                    .header("If-Modified-Since", "2011-12-01 12:27:13.000 GMT")
                    .header("If-Unmodified-Since", "2040-12-01 12:27:13.000 GMT")
                    .header("If-Match", "abc")
                    .header("If-None-Match", "abc")
                    .accept("application/oct-stream").get(ClientResponse.class);

            InputStream in = response.getEntityInputStream();
            OutputStream output = new FileOutputStream(LOCAL_PATH);

            int len;
            byte[] aa = new byte[1024];
            while((len = in.read(aa))!=-1){
                output.write(aa,0,len);
            }
            System.out.println("Output from Server .... \n");
            System.out.println("===smallFileDownLoad===getStatus======"+response.getStatus());
            System.out.println("===smallFileDownLoad=========\n"+response.toString());
            in.close();
            output.close();
            response.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void bigFileDownLoad(long startPosition,long endPosition){

        RandomAccessFile accessFile = null;
        try {
            accessFile = new RandomAccessFile(LOCAL_PATH, "rwd");
            accessFile.seek(startPosition);
            WebResource webResource = client.resource(DOWNLOADPATH);
            ClientResponse response = webResource
                    .header("User-Name", USER_NAME)
                    .header("Password", PASSWORD)
                    .header("If-Modified-Since", "2011-12-01 12:27:13.000 GMT")
                    .header("If-Unmodified-Since", "2040-12-01 12:27:13.000 GMT")
                    .header("If-Match", "abc")
                    .header("If-None-Match", "abc")
                    .header("Range", "bytes=" + startPosition + "-" + endPosition)
                    .accept("application/oct-stream").get(ClientResponse.class);
            writeTo(accessFile, response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeTo(RandomAccessFile accessFile, ClientResponse response){
        InputStream in = null;
        try {
            int len = -1;
            in = response.getEntityInputStream();
            byte[] buffer = new byte[1024*1024*2];
            while ((len = in.read(buffer)) != -1){
                accessFile.write(buffer, 0, len);
            }
            System.out.println("Content-Range :"+response.getHeaders().get("Content-Range").get(0));
            System.out.println(response.getStatus());
            System.out.println(response.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) {
                    in.close();
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

    public static void main(String[] args) throws Exception {
        IP = "10.199.33.12";
        PORT = "8888";
        PASSWORD = "111111";
        USER_NAME = "dong";
        LOCAL_PATH = "d:\\qqqqq\\log355M.txt";
        BUCKET_NAME = "s3_feng";
        DOWNLOADFILENAME ="log355M.txt";

        DOWNLOADPATH = "http://"+IP+":"+PORT+"/"+BUCKET_NAME+"/"+DOWNLOADFILENAME+"?method=get";

        FileDownLoad fileDownLoad = new FileDownLoad();
        long fileLength = fileDownLoad.getFileLength(DOWNLOADFILENAME);

        System.out.println(fileLength+"----------------");

        boolean isBigFile = fileDownLoad.isBigFile(fileLength);
        if(isBigFile){

            RandomAccessFile accessFile = new RandomAccessFile(LOCAL_PATH, "rwd");
            accessFile.setLength(fileLength);
            accessFile.close();

            long n = fileLength % range == 0 ? fileLength / range : fileLength / range + 1;

            for(int i=0; i<n; i++){
                long startPosition = i * range;
                long endPosition;
                if(i==(n-1)){
                    endPosition=fileLength-1;
                }else{
                    endPosition = (i + 1) * range - 1;
                }
                fileDownLoad.bigFileDownLoad(startPosition,endPosition);
            }

        }else{
            fileDownLoad.smallFileDownLoad();
        }
    }
}

