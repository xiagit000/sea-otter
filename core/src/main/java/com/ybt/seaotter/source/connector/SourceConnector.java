package com.ybt.seaotter.source.connector;

import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.meta.Schema;
import io.github.melin.superior.common.relational.create.CreateTable;

public interface SourceConnector {
    String getName();
    String getFlinkArgs();
    String[] getSparkArgs();

    DataDefine getDataDefine(SourceConnector connector);
    Schema getSchema();
    boolean createSchema(Schema schema);
}
