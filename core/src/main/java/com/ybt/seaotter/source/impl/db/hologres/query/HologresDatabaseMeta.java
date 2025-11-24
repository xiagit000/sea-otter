package com.ybt.seaotter.source.impl.db.hologres.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.db.hologres.HologresConnector;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;
import com.ybt.seaotter.source.meta.database.TableMeta;

import java.sql.*;
import java.util.List;

public class HologresDatabaseMeta implements DatabaseMeta {

    private final HologresConnector connector;
    private final String database;

    public HologresDatabaseMeta(HologresConnector connector, String database) {
        this.connector = connector;
        this.database = database;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:postgresql://%s:%s/%s", connector.getHost(), connector.getPort(), database),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> tables() {
        String sql = "SELECT tablename FROM pg_tables where schemaname = 'public'";
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
        return new HologresTableMeta(connector, database, structureName);
    }
}
