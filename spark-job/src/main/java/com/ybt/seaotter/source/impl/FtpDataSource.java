package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import com.ybt.seaotter.utils.DownloadUtils;
import org.apache.spark.sql.*;

import java.util.Map;

public class FtpDataSource implements DataSource {

    private Map<String, String> argsMap;

    public FtpDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        String fileName = argsMap.get("ftp.path").substring(argsMap.get("ftp.path").lastIndexOf("/") + 1);
        String localFilePath = "/opt/bitnami/spark/tmp/download/".concat(argsMap.get("callback.tag")).concat("_").concat(fileName);
        DownloadUtils.downloadByFtp(argsMap.get("ftp.host"), Integer.parseInt(argsMap.get("ftp.port")),
                argsMap.get("ftp.username"), argsMap.get("ftp.password"), argsMap.get("ftp.path"), localFilePath);
        return spark.read().option("header", "true").option("delimiter", argsMap.get("ftp.separator")).csv(localFilePath);
    }

    @Override
    public void writeData(Dataset<Row> data) {}
}
