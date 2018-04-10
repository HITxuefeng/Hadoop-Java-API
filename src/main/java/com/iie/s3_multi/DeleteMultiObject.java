package com.iie.s3_multi;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class DeleteMultiObject {

    static final Client client = Client.create();
    static byte[] bt = new byte[4096];
    static FileInputStream in;

    public static void main(String[] args) {
//		String ip = args[0];
//		String port = args[1];
//		String bucketName = args[3];
//		String userName =args[4];
//		String password =args[5];

        String ip = "172.16.5.1";
        String port = "9999";
        String bucketName = "zpb";
        String userName ="zhou" ;
        String password ="111111" ;

        run(ip, port,bucketName, userName, password);
    }
    public static void run(String ip,String port,String bucketName,String userName,String password) {

        try {
            File file = new File("test.xml");
            WebResource webResource = client.resource("http://" + ip + ":"+port+"/" + bucketName + "?method=del");
            ClientResponse response = webResource
                    .header("User-Name", userName)
                    .header("Password", password)
                    .type("application/oct-stream")
                    .entity(file)
                    .post(ClientResponse.class);

            System.out.print(response.getStatus()+" ");
            System.out.println(response.getStatus());

            InputStream in = response.getEntityInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            StringBuffer sb = new StringBuffer();
            String len = null;
            while((len=br.readLine())!=null){
                sb.append(len);
            }
            System.out.println(sb.toString());
            response.close();
            client.destroy();

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}
