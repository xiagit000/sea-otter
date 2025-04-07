package com.ybt.seaotter.source.impl.db.oracle.migration;

import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.oracle.OracleConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.utils.StarRocksUtils;
import io.github.melin.superior.common.relational.create.CreateTable;
import io.github.melin.superior.common.relational.table.ColumnRel;
import io.github.melin.superior.parser.oracle.OracleSqlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class OracleStarrocksTableMigrator implements DataMigrator {

    private final OracleConnector source;
    private final StarrocksConnector sink;
    private final Logger logger = LoggerFactory.getLogger(OracleStarrocksTableMigrator.class);

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
        String createTableSQL = StarRocksUtils.generateTableCreateSql(statement, sink.getTable(), this::convertColumnType);
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            logger.info("Oracle create table sql: {}", createTableSQL);
            System.out.println(createTableSQL);
            sinkStatement.execute(createTableSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }

    public String convertColumnType(String type) {
        switch (type.toLowerCase()) {
            case "timestamp":
                return "datetime";
            case "number":
                return "int";
            default:
                return type.replace("VARCHAR2", "varchar");
        }
    }
}
