package com.ybt.seaotter.source.impl.mysql.migration;

import com.google.common.collect.ImmutableMap;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.starrocks.StarrocksConnector;

public class MysqlDefine implements DataDefine {
    private MysqlConnector source;
    private SourceConnector sink;
    private final String MYSQL_DRIVER_JAR = "mysql-connector-java-8.0.28.jar";

    public MysqlDefine(MysqlConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    public DataMigrator getMigrator() {
        return ImmutableMap.of(
                DataSourceType.MYSQL.name().concat("-").concat(DataSourceType.STARROCKS.name()),
                new MysqlStarrocksTableMigrator(source, (StarrocksConnector) sink))
                .get(source.getName().concat("-").concat(sink.getName()));
    }

    @Override
    public String getDriverJar() {
        return MYSQL_DRIVER_JAR;
    }
}
