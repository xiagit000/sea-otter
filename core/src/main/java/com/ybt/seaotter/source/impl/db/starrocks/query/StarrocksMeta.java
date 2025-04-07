package com.ybt.seaotter.source.impl.db.starrocks.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.meta.database.DBMeta;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class StarrocksMeta implements DBMeta {

    private final StarrocksConnector connector;

    private final String[] filterDatabases = new String[]{"information_schema", "mysql", "performance_schema", "sys"};

    public StarrocksMeta(StarrocksConnector connector, SeaOtterConfig config) {
        this.connector = connector;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                .format("jdbc:mysql://%s:%s/%s", connector.getHost(), connector.getRpcPort(), "information_schema"),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> databases() {
        String sql = "SHOW DATABASES";
        List<String> databases = Lists.newArrayList();
        try (Connection connection =  getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                String database = resultSet.getString("Database");
                if (!Arrays.asList(filterDatabases).contains(database)) {
                    databases.add(database);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return databases;
    }

    @Override
    public DatabaseMeta database(String database) {
        return new StarrocksDatabaseMeta(connector, database);
    }


}
