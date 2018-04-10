package com.iie.s3.demo;

import java.io.File; 
import java.io.FileInputStream;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class  PostOneFileJersey implements Runnable{

	int id;
	static String userName;      
	static String passwd;         
	static String uploadFilePath;
	static String BUCKETNAME;
	static String OBJECTNAME;
	static int num;
	static int fornum;//循环次数
	
	final Client client = Client.create();
	byte[] bt = new byte[8192];   //8K
	
	FileInputStream in;
	

	public static void main(String[] args) throws InterruptedException {
		
//		userName=args[0];
//		passwd=args[1];
//		uploadFilePath=args[2];
//		BUCKETNAME=args[3];
//		OBJECTNAME=args[4];
//		num = Integer.parseInt(args[5]);
//		fornum=Integer.parseInt(args[6]);
		
		userName ="dong" ;
		passwd ="111111" ;
		uploadFilePath = "d:\\xxxxx\\feng.jpg";
//		BUCKETNAME = "http://10.199.33.12:8888/s3_feng/";
//		OBJECTNAME = "feng.jpg";
		num = 1;
		fornum=1;

		MultiThread();
	} 
	
	public static synchronized void  MultiThread() throws InterruptedException{
		for (int i = 0; i < num; i++) {
			Thread iThread =new Thread(new PostOneFileJersey(i));
			iThread.start();
		}
	}


	public PostOneFileJersey(int id){
		this.id=id;
	}
	
	@Override
	public void run() {
		try {

			String upload = "put";
			File file = new File(uploadFilePath);
			if(file.length() > 20971520){
				System.out.println("-------------------");
				upload = "append";
			}
//			  File file = new File(uploadFilePath);
				WebResource webResource = client.resource("http://10.199.33.12:8888/s3_feng/5555.jpg?method="+upload);
    			ClientResponse response = webResource
    					.header("User-Name", userName)
    					.header("Password", passwd)
						.entity(file)
    					.type("application/oct-stream").post(ClientResponse.class);
    			response.getClient();
    			response.getStatus();
    			System.out.println(response.getStatus());
    			response.close();
            client.destroy();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
