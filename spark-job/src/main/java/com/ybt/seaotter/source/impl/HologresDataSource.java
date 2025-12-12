package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import org.apache.spark.sql.*;

import java.util.Map;

public class HologresDataSource implements DataSource {

    private Map<String, String> argsMap;

    public HologresDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        return null;
    }

    @Override
    public void writeData(Dataset<Row> data) {
        data.write()
                .format("hologres")
                .option("username", argsMap.get("hologres.username"))
                .option("password", argsMap.get("hologres.password"))
                .option("endpoint", String.format("%s:%s", argsMap.get("hologres.host"), argsMap.get("hologres.port")))
                .option("database", argsMap.get("hologres.database"))
                .option("table", argsMap.get("hologres.table"))
                .mode(SaveMode.Append)
                .save();
    }
}
