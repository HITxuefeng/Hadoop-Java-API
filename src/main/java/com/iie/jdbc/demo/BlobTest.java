package com.iie.jdbc.demo;

import java.io.*;
import java.sql.*;

public class BlobTest {

	public static void main(String args[]) throws Exception {

		String file = "C:\\444.jpg";
		BlobTest test = new BlobTest();
		test.save(new File(file));
		//test.getMp3("blob");

	}

	public Connection getConnection() throws Exception{
		String url = "jdbc:mysql://10.199.33.13:3306/test";
		String user = "root";
		String password = "111111";
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}

	/**
	 *
	 * @param file  需要传入数据库的文件
	 * @throws Exception
	 */
	public void save(File file) throws Exception{
		Connection conn = getConnection();
		String sql = "insert into test_ha(c_time,c_pcode,c_nodeip,c_protocol,c_sip,c_dip,c_domain) values(?,?,?,?,?,?,?)";
		//insert into T_LOB (A, B, C) values (1, 'clob测试',to_blob('3456'));
		PreparedStatement prest = conn.prepareStatement(sql);
		String filename=file.getName();
		prest.setString(1, "20170910");
		prest.setString(2, "sushil");
		prest.setInt(3,20170808);
		prest.setInt(4,17);
		prest.setInt(5,20170808);
		prest.setInt(6,20170808);
		prest.setString(7, "201708098");
		FileInputStream fis = new FileInputStream(file);
		//prest.setBlob(2, fis,file.length());//第二个参数需要一个InputStream
		prest.execute();   //执行
	}

	/**
	 *
	 * @param filename  列的值，同时是文件名
	 * @throws Exception
	 */
	public void getMp3(String filename) throws Exception{
		Connection conn = getConnection();
		String sql = "select * from tb_blob where name= ?";
		PreparedStatement prest = conn.prepareStatement(sql);
		prest.setString(1, filename);
		ResultSet rs = prest.executeQuery();
		while(rs.next()){
			Blob  bl = rs.getBlob("myfile");//数据保存在"myfile"，这里则是取出这里保存的数据。
			InputStream is = bl.getBinaryStream();  //查看blob,可以通过流的形式取出来。
			BufferedInputStream buffis = new BufferedInputStream(is);
			//保存到buffout，就工程目录下的filename的文件
			BufferedOutputStream buffout = new BufferedOutputStream(new FileOutputStream(filename));
			byte[] buf= new byte[1024000];
			int len = buffis.read(buf, 0, 1024000);
			while(len>0){
				buffout.write(buf);
				len=buffis.read(buf, 0, 1024000);
			}
			buffout.flush();
			buffout.close();
			buffis.close();
		}
	}
}

