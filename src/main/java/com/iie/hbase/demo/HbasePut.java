package com.iie.hbase.demo;

//import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.DriverManager;

import static org.apache.hadoop.hbase.client.ConnectionFactory.createConnection;

public class HbasePut {


    public static Configuration config = null;

    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.199.33.12:2181");
        System.out.println("成功连接ZK");
    }

    /**
     * 创建Table
     *
     * @param tableName 表名
     * @param family    列族
     */
    public static void createTable(String tableName, String[] family) {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            try (Admin admin = connection.getAdmin()) {

                for (int i = 0; i < family.length; i++) {
                    table.addFamily(new HColumnDescriptor(family[i]));
                }
                if (admin.tableExists(TableName.valueOf(tableName))) {
                    System.out.println("Table Exists!!");
                    System.exit(0);
                } else {
                    admin.createTable(table);
                    System.out.println("Create Table Success!!! Table Name :[ " + tableName + " ]");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    /**
     * 添加数据
     *
     * @param rowKey    rowKey
     * @param tableName 表名
     * @param column    列名
     * @param value     值
     */
    public static void addData(String rowKey, String tableName, String[] column, String[] value) {
        try (Connection connection = createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));//存储到Hbase时都要转化为byte数组的形式
            HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
            for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString();
                if (familyName.equals("userinfo")) {
                    for (int j = 0; j < column.length; j++) {
                        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
                    }
                }
                table.put(put);
                System.out.println("Add Data Success!!!-");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String[] family = {"userinfo"};
        HbasePut.createTable("zxf",family);
        String[] column = {"name", "age", "email", "phone"};
        String[] value={"zengxuefeng","24","1564665679@qq.com","18463101815"};
        HbasePut.addData(
                "user","zxf",column,value);
    }


}
