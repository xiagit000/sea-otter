package com.ybt.seaotter.source;

import com.ybt.seaotter.source.impl.*;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public enum DataSourceType {
    MYSQL,
    DM,
    ORACLE,
    FTP,
    SFTP,
    STARROCKS,
    HOLOGRES;

    public static DataSource getDataSource(String source, Map<String, String> argMap) {
        DataSourceType dataSourceType = DataSourceType.valueOf(source);
        switch (dataSourceType) {
            case MYSQL:
                return new MysqlDataSource(argMap);
            case DM:
                return new DmDataSource(argMap);
            case ORACLE:
                return new OracleDataSource(argMap);
            case FTP:
                return new FtpDataSource(argMap);
            case SFTP:
                return new SftpDataSource(argMap);
            case STARROCKS:
                return new StarRocksDataSource(argMap);
            case HOLOGRES:
                return new HologresDataSource(argMap);
            default:
                return null;
        }
    }


}
