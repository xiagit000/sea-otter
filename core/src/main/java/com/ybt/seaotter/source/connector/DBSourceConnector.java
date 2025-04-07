package com.ybt.seaotter.source.connector;

import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.meta.database.DBMeta;
import io.github.melin.superior.common.relational.create.CreateTable;

public interface DBSourceConnector extends SourceConnector {
    DBMeta getMeta(SeaOtterConfig config);
    String getDatabase();
    String getTable();
    SourceConnector setDatabase(String database);
    SourceConnector setTable(String table);
//    String getCreateTableSql();
//    CreateTable getCreateTable();
}
