package com.iie.jdbc.demo;

import java.awt.image.BufferedImage;
import java.io.*;
import java.sql.*;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;


public class hiveDemo {
    public static void main(String args[]) throws SQLException, ClassNotFoundException, IOException {
//        String sql = "select * from test.biao";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager
                .getConnection("jdbc:hive2://10.128.77.107:10000/profile?useUnicode=true&characterEncoding=UTF-8","hive","111111");//10.221.1.1
        System.out.println("got connection");
//        Statement stmt = con.createStatement();
//
//        ResultSet rs = stmt.executeQuery(sql);
//        while (rs.next()){
//
//            System.out.println("11111");




//            ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
//            InputStream input=rs.getBinaryStream(1);
//            byte[] buff=new byte[102400];
//            int numBytesRead = 0;
//            try {
//                while ((numBytesRead = input.read(buff)) != -1) {
//                    swapStream.write(buff, 0, numBytesRead);
//                }
//
//            BufferedImage bi1 =ImageIO.read((ImageInputStream) swapStream);
//            File w2 = new File("d:\\xxxxx\\QQ.png");
//            ImageIO.write(bi1, "jpg", w2);
//
//        }catch (IOException e){
//                e.printStackTrace();
//            }
//        }
    }
}
