package com.iie.s3_multi;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.iie.s3_multi.Int2byteArray;

public class PostMultiFileJersey {

    static final Client client = Client.create();
    static byte[] bt = new byte[4096];
    static FileInputStream in;

    public static void main(String[] args) {
//		String ip = args[0];
//		String port = args[1];
//		String uploadFilePath = args[2];
//		String bucketName = args[3];
//		String userName =args[4];
//		String password =args[5];

        String ip = "172.16.5.1";
        String port = "9999";
        String bucketName = "zpb";
        String userName ="zhou" ;
        String password ="111111" ;
        String uploadFilePath ="D:\\zpb\\testdata";

        run(ip, port,uploadFilePath,bucketName, userName, password);
    }
    public static void run(String ip,String port,String uploadFilePath,String bucketName,String userName,String password) {

        ArrayList<String> filelist = getFiles(uploadFilePath);
        int objNum = filelist.size();
        try {
            byte[] b = getout(filelist);
            WebResource webResource = client.resource("http://" + ip + ":"+port+"/" + bucketName + "?method=put");
            ClientResponse response = webResource
                    .header("User-Name", userName)
                    .header("Password", password)
                    .header("KeyNums", objNum)
                    .type("application/oct-stream")
                    .post(ClientResponse.class,b);

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

    public static byte[] getout(ArrayList<String> la) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Iterator<String> items = la.iterator();
        while (items.hasNext()) {
            String localFile = items.next();
            Int2byteArray iba = new Int2byteArray();
            File file = new File(localFile);
            String name = file.getName();

            int n1 = name.getBytes().length;
            byte[] b1 = iba.i2ba(n1);
            byte[] b2 = name.getBytes();
            byte[] b4 = readFile(localFile);

            int n2 = b4.length;
            byte[] b3 = iba.i2ba(n2);

            out.write(b1,0,b1.length);

            out.write(b2,0,b2.length);

            out.write(b3,0,b3.length);

            out.write(b4,0,b4.length);
        }

        byte[] b5 = out.toByteArray();

        return b5;
    }

    public static byte[] readFile(String localfile) throws IOException {
        FileInputStream in;
        ByteArrayOutputStream out;

        in = new FileInputStream(localfile);
        out = new ByteArrayOutputStream();
        int len;
        byte[] bt = new byte[1024];
        while ((len = in.read(bt)) != -1) {
            out.write(bt, 0, len);
        }
        byte[] contents = out.toByteArray();
        in.close();
        return contents;
    }

    public static ArrayList<String> getFiles(String filePath) {
        ArrayList<String> la = new ArrayList<String>();
        File root = new File(filePath);
        File[] files = root.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                getFiles(file.getAbsolutePath());
            } else {
                la.add(file.getAbsolutePath());
            }
        }

        return la;
    }
}
