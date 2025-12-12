package com.ybt.seaotter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataPipeline {
    public static void main(String[] args) {
        // 创建 Spark 会话
        SparkSession spark = SparkSession.builder()
                .appName("DataLoader")
                .master("spark://localhost:7077")
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

        // 从达梦数据库读取数据
//        Dataset<Row> damengData = spark.read()
//                .format("jdbc")
//                .option("url", "jdbc:dm://172.16.5.101:5236/SYSDBA")
//                .option("dbtable", "BANK_USER")
//                .option("user", "SYSDBA")
//                .option("password", "Dameng111")
//                .load();

        mysqlData.write().format("jdbc")
                .option("url", "jdbc:postgresql://hgcn-xxx.yyy.zzz:80/db001")
                .option("driver", "org.postgresql.Driver")
                .option("dbtable", "tb001")
                .option("user", "Lxxxxxxxx")
                .option("password", "Dyyyyyyyyyyyyyyyyyyyy")
                .mode(SaveMode.Append)
                .save();

        // 将数据存储到 StarRocks
//        saveToStarRocks(mysqlData, "task");
//        saveToStarRocks(damengData, "user");
        spark.stop();
    }

    // 将数据保存到 StarRocks
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
