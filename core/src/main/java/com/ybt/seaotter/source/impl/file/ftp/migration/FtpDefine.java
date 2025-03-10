package com.ybt.seaotter.source.impl.file.ftp.migration;

import com.google.common.collect.ImmutableMap;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.file.ftp.FtpConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;

public class FtpDefine implements DataDefine {
    private SourceConnector source;
    private SourceConnector sink;
    private final String DRIVER_JAR = null;

    public FtpDefine(SourceConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    @Override
    public DataMigrator getMigrator() {
        return ImmutableMap.of(
                        DataSourceType.FTP.name().concat("-").concat(DataSourceType.STARROCKS.name()),
                        new FtpStarrocksMigrator((FtpConnector) source, (StarrocksConnector) sink))
                .get(source.getName().concat("-").concat(sink.getName()));
    }

    @Override
    public String getDriverJar() {
        return DRIVER_JAR;
    }
}
