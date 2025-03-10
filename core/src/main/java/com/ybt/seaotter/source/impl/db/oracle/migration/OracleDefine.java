package com.ybt.seaotter.source.impl.db.oracle.migration;

import com.google.common.collect.ImmutableMap;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.oracle.OracleConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;

public class OracleDefine implements DataDefine {

    private OracleConnector source;
    private SourceConnector sink;
    private final String DRIVER_JAR = "ojdbc8-21.1.0.0.jar";

    public OracleDefine(OracleConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    @Override
    public DataMigrator getMigrator() {
        return ImmutableMap.of(
                        DataSourceType.ORACLE.name().concat("-").concat(DataSourceType.STARROCKS.name()),
                        new OracleStarrocksTableMigrator(source, (StarrocksConnector) sink))
                .get(source.getName().concat("-").concat(sink.getName()));
    }

    @Override
    public String getDriverJar() {
        return DRIVER_JAR;
    }
}
