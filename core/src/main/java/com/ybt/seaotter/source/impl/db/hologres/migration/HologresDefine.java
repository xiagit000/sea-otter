package com.ybt.seaotter.source.impl.db.hologres.migration;

import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.hologres.HologresConnector;

public class HologresDefine implements DataDefine {
    private final HologresConnector source;
    private final SourceConnector sink;

    public HologresDefine(HologresConnector source, SourceConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    public DataMigrator getMigrator() {
        return new StarrocksHologresTableMigrator(source, (HologresConnector) sink);
    }

    @Override
    public String getDriverJar() {
        return "mysql-connector-java-8.0.28.jar";
    }
}
