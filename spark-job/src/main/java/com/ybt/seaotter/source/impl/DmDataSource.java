package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import org.apache.spark.sql.*;

import java.util.Map;

public class DmDataSource implements DataSource {

    private Map<String, String> argsMap;

    public DmDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        DataFrameReader reader = spark.read()
                .format("jdbc")
                .option("url", String.format("jdbc:dm://%s:%s/%s", argsMap.get("dm.host"),
                        argsMap.get("dm.port"), argsMap.get("dm.database")))
                .option("dbtable", argsMap.get("dm.table"))
                .option("user", argsMap.get("dm.username"))
                .option("password", argsMap.get("dm.password"));
        if (argsMap.get("upsertColumn") != null && !argsMap.get("upsertColumn").isEmpty()) {
            reader.option("where", String.format("%s >= %s", argsMap.get("upsertColumn"), argsMap.get("columnVal")));
        }
        return reader.load();
    }

    @Override
    public void writeData(Dataset<Row> data) {}
}
