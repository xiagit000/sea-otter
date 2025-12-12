package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import org.apache.spark.sql.*;

import java.util.Map;

public class MysqlDataSource implements DataSource {

    private Map<String, String> argsMap;

    public MysqlDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        DataFrameReader reader = spark.read()
                .format("jdbc")
                .option("url", String.format("jdbc:mysql://%s:%s/%s", argsMap.get("mysql.host"),
                        argsMap.get("mysql.port"), argsMap.get("mysql.database")))
                .option("dbtable", argsMap.get("mysql.table"))
                .option("user", argsMap.get("mysql.username"))
                .option("password", argsMap.get("mysql.password"));
        if (argsMap.get("upsertColumn") != null && !argsMap.get("upsertColumn").isEmpty()) {
            reader.option("where", String.format("%s >= %s", argsMap.get("upsertColumn"), argsMap.get("columnVal")));
        }
        return reader.load();
    }

    @Override
    public void writeData(Dataset<Row> data) {}
}
