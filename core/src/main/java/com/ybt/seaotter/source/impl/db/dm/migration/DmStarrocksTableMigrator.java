package com.ybt.seaotter.source.impl.db.dm.migration;

import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.dm.DmConnector;
import com.ybt.seaotter.source.impl.db.dm.sql.ColumnRel;
import com.ybt.seaotter.source.impl.db.dm.sql.CreateTable;
import com.ybt.seaotter.source.impl.db.dm.sql.DmSqlHelper;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.utils.StarRocksUtils;

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
        String sql = String.format("SELECT DBMS_METADATA.GET_DDL('TABLE', '%s') FROM DUAL", source.getTable());
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
        String createTableSQL = StarRocksUtils.generateTableCreateSql(statement, sink.getTable(), this::convertColumnType, sink.getReplicationNum());
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            System.out.println(createTableSQL);
            sinkStatement.execute(createTableSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }

    public String convertColumnType(String mysqlType) {
        String type = mysqlType.toLowerCase();
        if (type.startsWith("datetime") || type.startsWith("timestamp")) {
            return "datetime";
        }
        return type;
    }
}
