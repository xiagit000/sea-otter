package com.ybt.seaotter.source.impl.db.oracle;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.impl.db.oracle.migration.OracleDefine;
import com.ybt.seaotter.source.impl.db.oracle.query.OracleMeta;
import com.ybt.seaotter.source.meta.database.DBMeta;

import java.util.Arrays;
import java.util.stream.Collectors;

public class OracleConnector implements DBSourceConnector {

    private String host;
    private Integer port;
    private String sid;
    private String username;
    private String password;
    private String database;
    private String table;

    @Override
    public DBMeta getMeta(SeaOtterConfig config) {
        return new OracleMeta(this, config);
    }

    @Override
    public String getName() {
        return DataSourceType.ORACLE.name();
    }

    @Override
    public String getFlinkArgs() {
        return Arrays.stream(getSparkArgs()).collect(Collectors.joining(" "));
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--oracle.host %s", host),
                String.format("--oracle.port %s", port),
                String.format("--oracle.username %s", username),
                String.format("--oracle.password %s", password),
                String.format("--oracle.sid %s", sid),
                String.format("--oracle.database %s", database),
                String.format("--oracle.table %s", table),
        };
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new OracleDefine(this, sink);
    }

    public String getHost() {
        return host;
    }

    public OracleConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public OracleConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getSid() {
        return sid;
    }

    public OracleConnector setSid(String sid) {
        this.sid = sid;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public OracleConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public OracleConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public OracleConnector setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTable() {
        return table;
    }

    public OracleConnector setTable(String table) {
        this.table = table;
        return this;
    }
}
