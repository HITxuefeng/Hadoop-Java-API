package com.iie.jdbc.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class hive_binary {
    public static void main() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager
                .getConnection("jdbc:hive2://10.199.33.14:10000/healthmonitor?useUnicode=true&characterEncoding=UTF-8","hive","111111");//10.221.1.1
        System.out.println("got connection");
        String sql = "select * from healthmonitor.tb_checklist";
        Statement stmt = con.createStatement();

    }
}
