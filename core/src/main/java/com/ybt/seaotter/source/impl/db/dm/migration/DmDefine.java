package com.ybt.seaotter.source.impl.db.dm.migration;

import com.google.common.collect.ImmutableMap;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.dm.DmConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;

public class DmDefine implements DataDefine {
    private SourceConnector source;
    private SourceConnector sink;
    private final String DM_DRIVER_JAR = "DmJdbcDriver18-8.1.3.140.jar";

    public DmDefine(SourceConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    public DataMigrator getMigrator() {
        return ImmutableMap.of(
                DataSourceType.DM.name().concat("-").concat(DataSourceType.STARROCKS.name()),
                new DmStarrocksTableMigrator((DmConnector) source, (StarrocksConnector) sink))
                .get(source.getName().concat("-").concat(sink.getName()));
    }

    @Override
    public String getDriverJar() {
        return DM_DRIVER_JAR;
    }
}
