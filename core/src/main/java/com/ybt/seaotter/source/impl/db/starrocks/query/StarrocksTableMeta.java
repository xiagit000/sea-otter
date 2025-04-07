package com.ybt.seaotter.source.impl.db.starrocks.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.db.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.meta.database.TableMeta;

import java.sql.*;
import java.util.List;

public class StarrocksTableMeta implements TableMeta {

    private final StarrocksConnector connector;
    private final String tableName;
    private final String database;

    public StarrocksTableMeta(StarrocksConnector connector, String database, String tableName) {
        this.connector = connector;
        this.tableName = tableName;
        this.database = database;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:mysql://%s:%s/%s", connector.getHost(), connector.getRpcPort(), database),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> columns() {
        String sql = String.format("DESCRIBE %s", tableName);
        List<String> columns = Lists.newArrayList();
        try (Connection connection =  getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                columns.add(resultSet.getString("Field"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return columns;
    }

    @Override
    public List<List<String>> rows(Integer limit) {
        String sql = String.format("SELECT * FROM %s LIMIT %s", tableName, limit);
        List<List<String>> rows = Lists.newArrayList();
        try (Connection connection =  getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                List<String> row = Lists.newArrayList();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getString(i));
                }
                rows.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return rows;
    }

    @Override
    public void drop() {
        String sql = String.format("DROP TABLE %s", tableName);
        try {
            Connection connection =  getConnection();
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
