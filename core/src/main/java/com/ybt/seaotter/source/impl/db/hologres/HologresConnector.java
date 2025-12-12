package com.ybt.seaotter.source.impl.db.hologres;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.impl.db.hologres.migration.HologresDefine;
import com.ybt.seaotter.source.impl.db.hologres.query.HologresMeta;
import com.ybt.seaotter.source.meta.Schema;
import com.ybt.seaotter.source.meta.database.DBMeta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HologresConnector implements DBSourceConnector {
    private String host;
    private Integer port;
    private String username;
    private String password;
    private String database;
    private String table;

    public HologresConnector() {
    }

    public HologresConnector(String host, Integer port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public HologresConnector(String host, Integer port, String username, String password, String database, String table) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
    }

    public HologresConnector(String host, Integer port, String username, String password, String database, String table, Integer replicationNum) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
    }

    public String getHost() {
        return host;
    }

    public HologresConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public HologresConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public HologresConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public HologresConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public HologresConnector setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTable() {
        return table;
    }

    public HologresConnector setTable(String table) {
        this.table = table;
        return this;
    }

    @Override
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(String.format("jdbc:postgresql://%s:%s/%s", host, port, database),username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return DataSourceType.HOLOGRES.name();
    }

    @Override
    public String getFlinkArgs() {
        return String.join(" ", getSparkArgs());
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--hologress.host %s", host),
                String.format("--hologress.port %s", port),
                String.format("--hologress.username %s", username),
                String.format("--hologress.password %s", password),
                String.format("--hologress.database %s", database),
                String.format("--hologress.table %s", table),
        };
    }

    @Override
    public DBMeta getMeta(SeaOtterConfig config) {
        return new HologresMeta(this, config);
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new HologresDefine(this, sink);
    }

    @Override
    public Schema getSchema() {

        return null;
    }

    @Override
    public boolean createSchema(Schema schema) {
        return false;
    }

}
