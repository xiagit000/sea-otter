package com.ybt.seaotter.source.impl.oracle.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.oracle.OracleConnector;
import com.ybt.seaotter.source.meta.database.DBMeta;
import com.ybt.seaotter.source.meta.database.DatabaseMeta;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class OracleMeta implements DBMeta {

    private OracleConnector connector;

    public OracleMeta(OracleConnector connector, SeaOtterConfig config) {
        this.connector = connector;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:oracle:thin:@%s:%s:%s", connector.getHost(), connector.getPort(), connector.getSid()),
                connector.getUsername(), connector.getPassword());
    }

    @Override
    public List<String> databases() {
        String sql = "SELECT username FROM dba_users where account_status = 'OPEN' and default_tablespace not in ('SYSTEM', 'SYSAUX', 'USERS')";
        List<String> databases = Lists.newArrayList();
        try (Connection connection =  getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                databases.add(resultSet.getString("username"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SeaOtterException(e.getMessage());
        }
        return databases;
    }

    @Override
    public DatabaseMeta database(String containerName) {
        return new OracleDatabaseMeta(connector, containerName);
    }
}
