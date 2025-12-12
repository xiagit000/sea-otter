package com.ybt.seaotter.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DataSource {
    Dataset<Row> loadData(SparkSession spark);
    void writeData(Dataset<Row> data);
}
