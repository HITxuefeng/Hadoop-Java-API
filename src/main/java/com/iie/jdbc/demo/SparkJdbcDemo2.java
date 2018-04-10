package com.iie.jdbc.demo;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;

public class SparkJdbcDemo2 {

    public static void main(String args[]) {
        //String sql = args[0];//业务sql操作语句
//        String sql = "select * from healthmonitor.tb_checkitem where checkdate > '2017-10-20 15:19:00'";
        String sql = "select * from healthmonitor.tb_checkitem";
        try {
            //正常业务，spark jdbc连接hive进行sql操作
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection con = DriverManager
                    .getConnection("jdbc:hive2://10.199.33.14:10000/healthmonitor?useUnicode=true&characterEncoding=UTF-8","hive","111111");//10.221.1.1
            System.out.println("got connection");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sql);// executeQuery会返回结果的集合，否则返回空值
//            System.out.println("打印输出结果：");


            String url = "jdbc:mysql://10.199.33.11:3306/healthmonitor?useUnicode=true&characterEncoding=UTF-8";
            String user = "root";
            String password = "111111";
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);

            String sql1 = "insert into tb_checkitem(selfcheckid,itemid,devicetypecode,devicetype,devicespec,deviceid,itemdetailid,measurecode,value,measure,unit,sequence,scope,prompt,checkdate) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement prest = conn.prepareStatement(sql1);

            while (rs.next()) {
//            	System.out.println(rs);

//				System.out.println(rs.getString(0));


                prest.setString(1, rs.getString(1));
                prest.setString(2, rs.getString(2));
                prest.setString(3, rs.getString(3));
                prest.setString(4, rs.getString(4));
                prest.setString(5, rs.getString(5));
                prest.setString(6, rs.getString(6));
                prest.setString(7, rs.getString(7));
                prest.setString(8, rs.getString(8));
                prest.setString(9, rs.getString(9));
                prest.setString(10, rs.getString(10));
                prest.setString(11, rs.getString(11));
                prest.setInt(12, rs.getInt(12));
                prest.setString(13, rs.getString(13));
                prest.setString(14, rs.getString(14));
                prest.setString(15, rs.getString(15));
                prest.execute();   //执行

            }
            prest.close();
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
