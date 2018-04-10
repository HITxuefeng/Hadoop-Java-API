package com.iie.jdbc.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class mysqlDemo {
    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        String url = "jdbc:mysql://10.128.77.107:3306/test?useUnicode=true&characterEncoding=UTF-8";
        String user = "root";
        String password = "111111";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        System.out.println("数据库连接成功");
//        String sql1 = "replace into biao(name) values(?)";
//        PreparedStatement prest = conn.prepareStatement(sql1);
//        prest.setString(1,"你好");
//        System.out.println("----------插入数据库成功！");
//        prest.execute();
//        prest.close();
        conn.close();
    }
}
