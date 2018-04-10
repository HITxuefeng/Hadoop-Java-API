package com.iie.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseGet {
    public static Configuration config = null;

    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.199.33.12:2181");
        System.out.println("成功连接ZK");
    }
    //从hbase根据key获取value值
    public static Result getResult(String tableName, String rowKey) {
        Result result = null;
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))
        ) {
            Get get = new Get(Bytes.toBytes(rowKey));
            result = table.get(get);
            for (Cell cell : result.listCells()) {
                System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println("Timestamp:" + cell.getTimestamp());
                System.out.println("-------------------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args) throws IOException {
        getResult("zxf","user");
    }

}
