package com.ybt.seaotter.source.ddl;

public interface DataDefine {
    DataMigrator getMigrator();
    String getDriverJar();
}
