package com.ybt.seaotter.source.impl.db.starrocks.migration;

import com.google.common.collect.ImmutableMap;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;

public class StarrocksDefine implements DataDefine {
    private final StarrocksConnector source;
    private final SourceConnector sink;

    public StarrocksDefine(StarrocksConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    public DataMigrator getMigrator() {
        return new StarrocksStarrocksTableMigrator(source, (StarrocksConnector) sink);
    }

    @Override
    public String getDriverJar() {
        return "mysql-connector-java-8.0.28.jar";
    }
}
