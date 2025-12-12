package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import org.apache.spark.sql.*;

import java.util.Map;

public class StarRocksDataSource implements DataSource {

    private Map<String, String> argsMap;

    public StarRocksDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        return null;
    }

    @Override
    public void writeData(Dataset<Row> data) {
        data.write()
                .format("starrocks")
                .option("sink.buffer-flush.max-rows", "10000")
                .option("sink.buffer-flush.max-bytes", "2097152")
                .option("starrocks.fe.http.url", String.format("%s:%s", argsMap.get("starrocks.host"),
                        argsMap.get("starrocks.httpPort")))
                .option("starrocks.fe.jdbc.url", String.format("jdbc:mysql://%s:%s", argsMap.get("starrocks.host"),
                        argsMap.get("starrocks.rpcPort")))
                .option("starrocks.table.identifier", argsMap.get("starrocks.database").concat(".")
                        .concat(argsMap.get("starrocks.table")))
                .option("starrocks.user", argsMap.get("starrocks.username"))
                .option("starrocks.password", argsMap.get("starrocks.password"))
                .mode(SaveMode.Append)
                .save();
    }
}
