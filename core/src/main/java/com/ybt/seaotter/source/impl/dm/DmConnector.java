package com.ybt.seaotter.source.impl.dm;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.dm.migration.DmDefine;
import com.ybt.seaotter.source.impl.dm.query.DmMeta;
import com.ybt.seaotter.source.meta.database.DBMeta;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DmConnector implements DBSourceConnector {

    private String host;
    private Integer port;
    private String username;
    private String password;
    private String database;
    private String table;
    public DmConnector() {
    }

    public DmConnector(String host, Integer port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public DmConnector(String host, Integer port, String username, String password, String database) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public DmConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public DmConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public DmConnector setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public DmConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DmConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getTable() {
        return table;
    }

    public DmConnector setTable(String table) {
        this.table = table;
        return this;
    }

    @Override
    public String getName() {
        return DataSourceType.DM.name();
    }

    @Override
    public String getFlinkArgs() {
        return Arrays.stream(getSparkArgs()).collect(Collectors.joining(" "));
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--dm.host %s", host),
                String.format("--dm.port %s", port),
                String.format("--dm.username %s", username),
                String.format("--dm.password %s", password),
                String.format("--dm.database %s", database),
                String.format("--dm.table %s", table)
        };
    }

    @Override
    public DBMeta getMeta(SeaOtterConfig config) {
        return new DmMeta(this, config);
    }

    public DataMigrator migrateTableSchemaTo(SourceConnector sink) {
        return new DmDefine(this, sink).getMigrator();
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new DmDefine(this, sink);
    }

}
