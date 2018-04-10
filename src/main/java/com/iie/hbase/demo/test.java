package com.iie.hbase.demo;

import com.mysql.jdbc.Blob;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * 实现Hbase图片的存取(blob数据类型)
 * hbase java client!
 * Created by xuefeng on 2017/8/31.
 */

public class test {

	private static Configuration conf=null;
	private static String host = "10.199.33.12";
	private static String port = "2181";

	//连接参数
	public test(){
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", host);
		conf.set("habse.zookeeper.property.clientPort", port);
		conf.set("hbase.client.keyvalue.maxsize", "0");
		System.out.println("成功连接ZK");
	}

	//创建Hbase表
	private void createTable(String tableName){
		try {

			org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();

//			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.tableExists(TableName.valueOf(tableName))){
				System.out.println("table :"+tableName+"Exists!");
			}else{
				HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tableName));
				td.addFamily(new HColumnDescriptor("info"));
				admin.createTable(td);
				System.out.println("create table ："+tableName+" Success!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

//	public static void createTable(String tableName, String[] family) {
//		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
//		try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(conf)) {
//			try (Admin admin = connection.getAdmin()) {
//
//				for (int i = 0; i < family.length; i++) {
//					table.addFamily(new HColumnDescriptor(family[i]));
//				}
//				if (admin.tableExists(TableName.valueOf(tableName))) {
//					System.out.println("Table Exists!!");
//					System.exit(0);
//				} else {
//					admin.createTable ( table );
//					System.out.println("Create Table Success!!! Table Name :[ " + tableName + " ]");
//				}
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}



	//将blob数据类型put到Hbase上
	private int insert(String key,byte[]value){
		int flag=0;
		try {
			HTable table =new HTable(conf, "yjq".getBytes());
			Put put =new Put(key.getBytes());
			put.add("info".getBytes(), "photo2".getBytes(), value);
			table.put(put);
			table.flushCommits();
			table.close();
			System.out.println("插入hbase成功");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}


	//从Hbase获取图片并写入到本地！
	private int get(String key){
		int flag=0;
		try {
			HTable table = new HTable(conf, "yjq".getBytes());
			Get get = new Get(key.getBytes());
			Result result=table.get(get);
			KeyValue[] kv=result.raw();
			byte[] bs=new byte[kv.length];
			for(KeyValue kv1:result.raw()){
				bs=kv1.getValue();
			}
//			File file=new File("/root/feng.docx");
			File file=new File("C:\\xxxxx\\feng.txt");
			FileOutputStream fos=new FileOutputStream(file);
			fos.write(bs);
			fos.flush();
			fos.close();
			System.out.println("文档存入成功！");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}

//数据库的连接
	public static Connection getConnection() throws Exception{
		String url = "jdbc:mysql://10.199.33.13:3306/test";
		String user = "root";
		String password = "111111";
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = (Connection) DriverManager.getConnection(url, user, password);
		return conn;
	}


	public static void main(String args[]) throws Exception{
		//先从sql数据库获取数据，当作put时的参数，将BLOB数据类型转换成byte数组
		Connection conn = getConnection();
		String sql = "select * from test.tb_blob where name='feng.docx'";
        Statement stmt = (Statement) conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);// executeQuery会返回结果的集合，否则返回空值
        System.out.println("数据库查询成功：");
        rs.next();
        System.out.println(rs.getString(1));
        Blob bb=(Blob) rs.getBlob(2);
        byte[] b=bb.getBytes(1, (int)bb.length());

		test hbase=new test();
		hbase.createTable("yjq");
		hbase.insert("feng1", b);
		hbase.get("feng1");
	}

}
