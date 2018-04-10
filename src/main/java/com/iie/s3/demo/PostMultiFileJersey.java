package com.iie.s3.demo;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;


public class PostMultiFileJersey {

    static final Client client = Client.create();
    static byte[] bt = new byte[4096];
    static FileInputStream in;

    public static void main(String[] args) {
        //在服务器上以输入参数的方式
//		String ip = args[0];
//		String port = args[1];
//		String uploadFilePath = args[2];
//		String bucketName = args[3];
//		String userName =args[4];
//		String password =args[5];

        //在windows端
        String ip = "10.199.33.12";
        String port = "8888";
        String bucketName = "s3_feng11";
        String userName ="dong" ;
        String password ="111111" ;
        String uploadFilePath ="c:\\xxxxx";

        run(ip, port,uploadFilePath,bucketName, userName, password);
    }
    public static void run(String ip,String port,String uploadFilePath,String bucketName,String userName,String password) {

        ArrayList<String> filelist = getFiles(uploadFilePath);
        int objNum = filelist.size();
        Iterator<String> items = filelist.iterator();

        //有多少个文件就循环多少次，进行put
        while(items.hasNext()){
            String localFile = items.next();
            File file = new File(localFile);
            String name = file.getName();
            try {
                WebResource webResource = client.resource("http://" + ip + ":"+port+"/" + bucketName +"/"+name+ "?method=put");
                ClientResponse response = webResource
                        .header("User-Name", userName)
                        .header("Password", password)
                        .entity(file)
                        .type("application/oct-stream")
                        .post(ClientResponse.class);
                System.out.print(response.getStatus()+" ");
                System.out.println(response.getStatus());
                response.close();
                client.destroy();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //批量读文件
    public static ArrayList<String> getFiles(String filePath) {
        ArrayList<String> la = new ArrayList<String>();
        File root = new File(filePath);
        File[] files = root.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                getFiles(file.getAbsolutePath());
            } else {
                la.add(file.getAbsolutePath());
                System.out.println(file.getAbsolutePath());
            }
        }
        return la;
    }
}

