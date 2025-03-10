package com.ybt.seaotter.source.impl.file.sftp.migration;

import com.google.common.collect.ImmutableMap;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.impl.file.sftp.SftpConnector;

public class SftpDefine implements DataDefine {
    private final SourceConnector source;
    private final SourceConnector sink;
    private final String DRIVER_JAR = null;

    public SftpDefine(SourceConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    @Override
    public DataMigrator getMigrator() {
        return ImmutableMap.of(
                        DataSourceType.SFTP.name().concat("-").concat(DataSourceType.STARROCKS.name()),
                        new SftpStarrocksMigrator((SftpConnector) source, (StarrocksConnector) sink))
                .get(source.getName().concat("-").concat(sink.getName()));
    }

    @Override
    public String getDriverJar() {
        return DRIVER_JAR;
    }
}
