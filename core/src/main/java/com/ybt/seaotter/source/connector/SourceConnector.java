package com.ybt.seaotter.source.connector;

import com.ybt.seaotter.source.ddl.DataDefine;

public interface SourceConnector {
    String getName();
    String getFlinkArgs();
    String[] getSparkArgs();

    DataDefine getDataDefine(SourceConnector connector);
}
