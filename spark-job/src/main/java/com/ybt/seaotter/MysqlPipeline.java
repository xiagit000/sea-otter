package com.ybt.seaotter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MysqlPipeline {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Batch sync Mysql to Starrocks")
                .config("spark.sql.shuffle.partitions", "10")
                .getOrCreate();

        // 从 MySQL 读取数据
        Dataset<Row> mysqlData = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://47.96.22.250:33066/jd_data")
                .option("dbtable", "task")
                .option("user", "dbsql")
                .option("password", "$027@mydbsql")
                .load();

        // 将数据存储到 StarRocks
        saveToStarRocks(mysqlData, "task");
        spark.stop();
    }

    private static void saveToStarRocks(Dataset<Row> data, String targetTableName) {
        data.write()
                .format("starrocks")
                .option("starrocks.fe.http.url", "172.16.1.51:8080")
                .option("starrocks.fe.jdbc.url", "jdbc:mysql://172.16.1.51:9030")
                .option("starrocks.table.identifier", "data_warehouse.".concat(targetTableName))
                .option("starrocks.user", "root")
                .option("starrocks.password", "")
                .mode(SaveMode.Append)
                .save();
    }
}
