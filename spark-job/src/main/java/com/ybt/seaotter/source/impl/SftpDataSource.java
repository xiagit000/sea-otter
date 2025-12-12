package com.ybt.seaotter.source.impl;

import com.ybt.seaotter.source.DataSource;
import com.ybt.seaotter.utils.DownloadUtils;
import org.apache.spark.sql.*;

import java.util.Map;

public class SftpDataSource implements DataSource {

    private Map<String, String> argsMap;

    public SftpDataSource(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

    @Override
    public Dataset<Row> loadData(SparkSession spark) {
        String fileName = argsMap.get("sftp.path").substring(argsMap.get("sftp.path").lastIndexOf("/") + 1);
        String localFilePath = "/opt/bitnami/spark/tmp/download/".concat(argsMap.get("callback.tag")).concat("_").concat(fileName);
        DownloadUtils.downloadBySftp(argsMap.get("sftp.host"), Integer.parseInt(argsMap.get("sftp.port")),
                argsMap.get("sftp.username"), argsMap.get("sftp.password"), argsMap.get("sftp.path"), localFilePath);
        return spark.read().option("header", "true").option("delimiter", argsMap.get("ftp.separator")).csv(localFilePath);
    }

    @Override
    public void writeData(Dataset<Row> data) {}
}
