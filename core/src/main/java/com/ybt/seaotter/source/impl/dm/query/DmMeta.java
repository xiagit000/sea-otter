package com.ybt.seaotter.source.impl.dm.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.dm.DmConnector;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;
import com.ybt.seaotter.source.meta.database.DBMeta;

import java.sql.*;
import java.util.List;

public class DmMeta implements DBMeta {

    private DmConnector connector;

    private final String[] filterDatabases = new String[]{"information_schema", "mysql", "performance_schema", "sys"};

    public DmMeta(DmConnector connector, SeaOtterConfig config) {
        this.connector = connector;
    }

    private Connection getConnection(String database) throws SQLException {
        return DriverManager.getConnection(String
                .format("jdbc:dm://%s:%s/%s", connector.getHost(), connector.getPort(), database),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> databases() {
        String sql = "SELECT DISTINCT object_name FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'SCH'";
        List<String> databases = Lists.newArrayList();
        try (Connection connection =  getConnection(connector.getUsername());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                databases.add(resultSet.getString("object_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return databases;
    }

    @Override
    public DatabaseMeta database(String database) {
        return new DmDatabaseMeta(connector, database);
    }


}
