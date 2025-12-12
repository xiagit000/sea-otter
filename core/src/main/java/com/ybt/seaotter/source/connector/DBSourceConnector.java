package com.ybt.seaotter.source.connector;

import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.meta.database.DBMeta;
import io.github.melin.superior.common.relational.create.CreateTable;

import java.sql.Connection;

public interface DBSourceConnector extends SourceConnector {
    DBMeta getMeta(SeaOtterConfig config);
    String getDatabase();
    String getTable();
    DBSourceConnector setDatabase(String database);
    DBSourceConnector setTable(String table);
    Connection getConnection();
//    String getCreateTableSql();
//    CreateTable getCreateTable();
}
