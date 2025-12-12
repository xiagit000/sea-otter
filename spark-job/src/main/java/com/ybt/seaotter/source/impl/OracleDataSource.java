package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import org.apache.spark.sql.*;

import java.util.Map;

public class OracleDataSource implements DataSource {

    private Map<String, String> argsMap;

    public OracleDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        DataFrameReader reader = spark.read()
                .format("jdbc")
                .option("url", String.format("jdbc:oracle:thin:@%s:%s:%s", argsMap.get("oracle.host"),
                        argsMap.get("oracle.port"), argsMap.get("oracle.sid")))
                .option("dbtable", String.format("%s.%s", argsMap.get("oracle.database"), argsMap.get("oracle.table")))
                .option("user", argsMap.get("oracle.username"))
                .option("password", argsMap.get("oracle.password"))
                .option("driver", "oracle.jdbc.OracleDriver");
        if (argsMap.get("upsertColumn") != null && !argsMap.get("upsertColumn").isEmpty()) {
            reader.option("where", String.format("%s >= %s", argsMap.get("upsertColumn"), argsMap.get("columnVal")));
        }
        return reader.load();
    }

    @Override
    public void writeData(Dataset<Row> data) {}
}
