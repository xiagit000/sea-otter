package com.ybt.seaotter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataPipelineWithLake {

    private SparkSession spark;

    public DataPipelineWithLake() {
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        // 初始化 SparkSession
        this.spark = SparkSession.builder()
                .appName("test spark")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", "file:///D:/iceberg/warehouse") // 设置 Iceberg 的数据仓库路径
                .getOrCreate();
    }

    public Dataset<Row> sql(String sql) {
        return spark.sql(sql);
    }

    public static void main(String[] args) {
        DataPipelineWithLake dataPipeline = new DataPipelineWithLake();
//        dataPipeline.sql("CREATE TABLE local.db.employees (" +
//                "employee_id BIGINT, " +
//                "name STRING, " +
//                "department STRING, " +
//                "salary DOUBLE, " +
//                "hire_date DATE) " +
//                "USING iceberg");
        dataPipeline.sql("INSERT INTO local.db.employees VALUES " +
                "(1, 'Alice', 'Engineering', 85000, '2020-01-15'), " +
                "(2, 'Bob', 'HR', 65000, '2019-03-22'), " +
                "(3, 'Charlie', 'Marketing', 70000, '2018-10-13')");
        dataPipeline.sql("SELECT * FROM local.db.employees").show();
    }
}
