package com.ybt.seaotter.source.impl.mysql.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.mysql.MysqlConnector;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;
import com.ybt.seaotter.source.meta.database.DBMeta;

import java.sql.*;
import java.util.List;

public class MysqlMeta implements DBMeta {

    private MysqlConnector connector;

    private final String[] filterDatabases = new String[]{"information_schema", "mysql", "performance_schema", "sys"};

    public MysqlMeta(MysqlConnector connector, SeaOtterConfig config) {
        this.connector = connector;
    }

    private Connection getConnection(String database) throws SQLException {
        return DriverManager.getConnection(String
                .format("jdbc:mysql://%s:%s/%s", connector.getHost(), connector.getPort(), database),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> databases() {
        String sql = "select SCHEMA_NAME from schemata where SCHEMA_NAME not in ('information_schema', 'mysql', 'performance_schema', 'sys')";
        List<String> databases = Lists.newArrayList();
        try (Connection connection =  getConnection("information_schema");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                databases.add(resultSet.getString("SCHEMA_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return databases;
    }

    @Override
    public DatabaseMeta database(String database) {
        return new MysqlDatabaseMeta(connector, database);
    }


}
