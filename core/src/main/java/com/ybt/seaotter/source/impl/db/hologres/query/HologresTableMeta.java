package com.ybt.seaotter.source.impl.db.hologres.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.db.hologres.HologresConnector;
import com.ybt.seaotter.source.meta.database.TableMeta;

import java.sql.*;
import java.util.List;

public class HologresTableMeta implements TableMeta {

    private final HologresConnector connector;
    private final String tableName;
    private final String database;

    public HologresTableMeta(HologresConnector connector, String database, String tableName) {
        this.connector = connector;
        this.tableName = tableName;
        this.database = database;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:postgresql://%s:%s/%s", connector.getHost(), connector.getPort(), database),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> columns() {
        String sql = String.format("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' " +
                "AND table_name = '%s' ORDER BY ordinal_position", tableName);
        List<String> columns = Lists.newArrayList();
        try (Connection connection =  getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                columns.add(resultSet.getString("column_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return columns;
    }

    @Override
    public List<List<String>> rows(Integer limit) {
        String sql = String.format("SELECT * FROM public.%s LIMIT %s", tableName, limit);
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
        String sql = String.format("DROP TABLE public.%s", tableName);
        try {
            Connection connection =  getConnection();
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
