package com.ybt.seaotter.source.impl.db.starrocks;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.mysql.migration.MysqlDefine;
import com.ybt.seaotter.source.impl.db.starrocks.migration.StarrocksDefine;
import com.ybt.seaotter.source.impl.db.starrocks.query.StarrocksMeta;
import com.ybt.seaotter.source.meta.database.DBMeta;
import io.github.melin.superior.common.relational.create.CreateTable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;

public class StarrocksConnector implements DBSourceConnector {
    private String host;
    private Integer httpPort;
    private Integer rpcPort;
    private String username;
    private String password;
    private String database;
    private String table;

    public StarrocksConnector() {
    }

    public StarrocksConnector(String host, Integer httpPort, Integer rpcPort, String username, String password) {
        this.host = host;
        this.httpPort = httpPort;
        this.rpcPort = rpcPort;
        this.username = username;
        this.password = password;
    }

    public StarrocksConnector(String host, Integer httpPort, Integer rpcPort, String username, String password, String database, String table) {
        this.host = host;
        this.httpPort = httpPort;
        this.rpcPort = rpcPort;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
    }

    public String getHost() {
        return host;
    }

    public StarrocksConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public StarrocksConnector setHttpPort(Integer httpPort) {
        this.httpPort = httpPort;
        return this;
    }

    public Integer getRpcPort() {
        return rpcPort;
    }

    public StarrocksConnector setRpcPort(Integer rpcPort) {
        this.rpcPort = rpcPort;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public StarrocksConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public StarrocksConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public StarrocksConnector setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTable() {
        return table;
    }

    public StarrocksConnector setTable(String table) {
        this.table = table;
        return this;
    }

    @Override
    public String getName() {
        return DataSourceType.STARROCKS.name();
    }

    @Override
    public String getFlinkArgs() {
        return String.join(" ", getSparkArgs());
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--starrocks.host %s", host),
                String.format("--starrocks.httpPort %s", httpPort),
                String.format("--starrocks.rpcPort %s", rpcPort),
                String.format("--starrocks.username %s", username),
                String.format("--starrocks.password %s", password),
                String.format("--starrocks.database %s", database),
                String.format("--starrocks.table %s", table)
        };
    }

    @Override
    public DBMeta getMeta(SeaOtterConfig config) {
        return new StarrocksMeta(this, config);
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new StarrocksDefine(this, sink);
    }
}
