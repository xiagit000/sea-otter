package com.ybt.seaotter.source.impl.oracle.migration;

import com.github.melin.superior.sql.parser.mysql.MySqlHelper;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.mysql.migration.MysqlStarrocksTableMigrator;
import com.ybt.seaotter.source.impl.oracle.OracleConnector;
import com.ybt.seaotter.source.impl.starrocks.StarrocksConnector;
import io.github.melin.superior.common.relational.create.CreateTable;
import io.github.melin.superior.common.relational.table.ColumnRel;
import io.github.melin.superior.parser.oracle.OracleSqlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class OracleStarrocksTableMigrator implements DataMigrator {

    private OracleConnector source;
    private StarrocksConnector sink;
    private Logger logger = LoggerFactory.getLogger(OracleStarrocksTableMigrator.class);

    public OracleStarrocksTableMigrator(OracleConnector source, StarrocksConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    private Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:oracle:thin:@%s:%s:%s", source.getHost(), source.getPort(), source.getSid()),
                source.getUsername(), source.getPassword());
    }

    private Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:mysql://%s:%s/%s", sink.getHost(), sink.getRpcPort(), sink.getDatabase()),
                sink.getUsername(), sink.getPassword());
    }

    @Override
    public void migrate() {
        String sql = String.format("SELECT DBMS_METADATA.GET_DDL('TABLE', '%s', '%s') FROM DUAL", source.getTable(), source.getDatabase());
        String mysqlCreateSql = "";
        try (Connection connection =  getSourceConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                mysqlCreateSql = resultSet.getString(1);
            }
        } catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
        CreateTable statement = (CreateTable) OracleSqlHelper.parseStatement(mysqlCreateSql);
        String tableName = sink.getTable();
        List<ColumnRel> keyList = statement.getColumnRels().stream()
                .filter(column -> column.getPrimaryKey()).collect(Collectors.toList());
        String columnDefinitions = statement.getColumnRels().stream()
                .map(column -> column.getColumnName()
                        .replaceAll("\"", "") + " " + convertColumnType(column.getTypeName()))
                .collect(Collectors.joining(", "));
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (")
                .append(columnDefinitions);
        createTableSQL.append(")");
        if (keyList != null && keyList.size() > 0) {
            createTableSQL.append(" PRIMARY KEY (");
            createTableSQL.append(keyList.stream().map(column -> column.getColumnName()
                    .replaceAll("\"", "")).collect(Collectors.joining(", ")));
            createTableSQL.append(")");

            createTableSQL.append(" DISTRIBUTED BY HASH (");
            createTableSQL.append(keyList.stream().map(column -> column.getColumnName()
                    .replaceAll("\"", "")).collect(Collectors.joining(", ")));
            createTableSQL.append(")");
        }
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            String createSQL = createTableSQL.toString();
            logger.info("Oracle create table sql: {}", createSQL);
            System.out.println(createSQL);
            sinkStatement.execute(createSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }

    public String convertColumnType(String type) {
        switch (type.toLowerCase()) {
            case "number":
                return "int";
            default:
                return type.replace("VARCHAR2", "varchar");
        }
    }
}
