package com.ybt.seaotter.source.impl.dm.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.dm.DmConnector;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;
import com.ybt.seaotter.source.meta.database.TableMeta;

import java.sql.*;
import java.util.List;

public class DmDatabaseMeta implements DatabaseMeta {

    private DmConnector connector;
    private String database;

    public DmDatabaseMeta(DmConnector connector, String database) {
        this.connector = connector;
        this.database = database;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:dm://%s:%s/%s", connector.getHost(), connector.getPort(), database),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> tables() {
        String sql = String.format("SELECT table_name FROM dba_tables WHERE owner = '%s' and table_name " +
                "not in ('##PLAN_TABLE', '##HISTOGRAMS_TABLE')", database);
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
        return new DmTableMeta(connector, database, structureName);
    }
}
