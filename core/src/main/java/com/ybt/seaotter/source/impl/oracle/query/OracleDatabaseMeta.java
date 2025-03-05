package com.ybt.seaotter.source.impl.oracle.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.mysql.query.MysqlTableMeta;
import com.ybt.seaotter.source.impl.oracle.OracleConnector;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;
import com.ybt.seaotter.source.meta.database.TableMeta;

import java.sql.*;
import java.util.List;

public class OracleDatabaseMeta implements DatabaseMeta {

    private OracleConnector connector;
    private String database;

    public OracleDatabaseMeta(OracleConnector connector, String database) {
        this.connector = connector;
        this.database = database;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:oracle:thin:@%s:%s:%s", connector.getHost(), connector.getPort(), connector.getSid()),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> tables() {
        String sql = String.format("SELECT TABLE_NAME FROM DBA_TABLES WHERE TABLESPACE_NAME = '%s'", database);
        List<String> tables = Lists.newArrayList();
        try (Connection connection =  getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                tables.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return tables;
    }

    @Override
    public TableMeta table(String structureName) {
        return new OracleTableMeta(connector, database, structureName);
    }
}
