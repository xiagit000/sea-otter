package com.ybt.seaotter.source.impl.dm.migration;

import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.dm.DmConnector;
import com.ybt.seaotter.source.impl.dm.sql.ColumnRel;
import com.ybt.seaotter.source.impl.dm.sql.CreateTable;
import com.ybt.seaotter.source.impl.dm.sql.DmSqlHelper;
import com.ybt.seaotter.source.impl.starrocks.StarrocksConnector;

import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class DmStarrocksTableMigrator implements DataMigrator {

    private DmConnector source;
    private StarrocksConnector sink;

    public DmStarrocksTableMigrator(DmConnector source, StarrocksConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    private Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:dm://%s:%s/%s", source.getHost(), source.getPort(), source.getDatabase()),
                source.getUsername(), source.getPassword());
    }

    private Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:mysql://%s:%s/%s", sink.getHost(), sink.getRpcPort(), sink.getDatabase()),
                sink.getUsername(), sink.getPassword());
    }

    @Override
    public void migrate() {
        String sql = String.format("SELECT DBMS_METADATA.GET_DDL('TABLE', '%s') FROM DUAL;", source.getTable());
        String mysqlCreateSql = "";
        try (Connection connection =  getSourceConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                mysqlCreateSql = resultSet.getString(1);
            }
        } catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
        CreateTable statement = DmSqlHelper.parseCreateTableStatement(mysqlCreateSql);
        String tableName = sink.getTable();
        List<ColumnRel> keyList = statement.getColumnRels().stream().filter(column -> column.getPrimaryKey()).collect(Collectors.toList());
        String columnDefinitions = statement.getColumnRels().stream()
                .map(column -> column.getColumnName() + " " + column.getTypeName())
                .collect(Collectors.joining(", "));
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (")
                .append(columnDefinitions);
        createTableSQL.append(")");
        if (keyList != null && keyList.size() > 0) {
            createTableSQL.append(" PRIMARY KEY (");
            createTableSQL.append(keyList.stream().map(column -> column.getColumnName()).collect(Collectors.joining(", ")));
            createTableSQL.append(")");

            createTableSQL.append(" DISTRIBUTED BY HASH (");
            createTableSQL.append(keyList.stream().map(column -> column.getColumnName()).collect(Collectors.joining(", ")));
            createTableSQL.append(")");
        }
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            String createSQL = createTableSQL.toString();
            System.out.println(createSQL);
            sinkStatement.execute(createSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }
}
