package com.ybt.seaotter.source.impl.mysql;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.mysql.migration.MysqlDefine;
import com.ybt.seaotter.source.impl.mysql.query.MysqlMeta;
import com.ybt.seaotter.source.meta.database.DBMeta;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MysqlConnector implements DBSourceConnector {

    private String host;
    private Integer port;
    private String username;
    private String password;
    private String database;
    private String table;
    public MysqlConnector() {
    }

    public MysqlConnector(String host, Integer port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public MysqlConnector(String host, Integer port, String username, String password, String database) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public MysqlConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public MysqlConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public MysqlConnector setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public MysqlConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public MysqlConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getTable() {
        return table;
    }

    public MysqlConnector setTable(String table) {
        this.table = table;
        return this;
    }

    @Override
    public String getName() {
        return DataSourceType.MYSQL.name();
    }

    @Override
    public String getFlinkArgs() {
        return Arrays.stream(getSparkArgs()).collect(Collectors.joining(" "));
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--mysql.host %s", host),
                String.format("--mysql.port %s", port),
                String.format("--mysql.username %s", username),
                String.format("--mysql.password %s", password),
                String.format("--mysql.database %s", database),
                String.format("--mysql.table %s", table)
        };
    }

    @Override
    public DBMeta getMeta(SeaOtterConfig config) {
        return new MysqlMeta(this, config);
    }

    public DataMigrator migrateTableSchemaTo(SourceConnector sink) {
        return new MysqlDefine(this, sink).getMigrator();
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new MysqlDefine(this, sink);
    }

}
