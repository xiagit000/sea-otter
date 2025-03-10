package com.ybt.seaotter.source.impl.db.mysql.migration;

import com.github.melin.superior.sql.parser.mysql.MySqlHelper;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.utils.StarRocksUtils;
import io.github.melin.superior.common.relational.create.CreateTable;
import io.github.melin.superior.common.relational.table.ColumnRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlStarrocksTableMigrator implements DataMigrator {

    private MysqlConnector source;
    private StarrocksConnector sink;
    private Logger logger = LoggerFactory.getLogger(MysqlStarrocksTableMigrator.class);

    public MysqlStarrocksTableMigrator(MysqlConnector source, StarrocksConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    private Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:mysql://%s:%s/%s", source.getHost(), source.getPort(), source.getDatabase()),
                source.getUsername(), source.getPassword());
    }

    private Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:mysql://%s:%s/%s", sink.getHost(), sink.getRpcPort(), sink.getDatabase()),
                sink.getUsername(), sink.getPassword());
    }

    @Override
    public void migrate() {
        String sql = String.format("show create table %s", source.getTable());
        String mysqlCreateSql = "";
        try (Connection connection =  getSourceConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                // 假设查询的表有两个字段 id 和 name
                mysqlCreateSql = resultSet.getString(2);
            }
        } catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
        CreateTable statement = (CreateTable) MySqlHelper.parseStatement(mysqlCreateSql);
        String createTableSQL = StarRocksUtils.generateTableCreateSql(statement, sink.getTable(), this::convertColumnType);
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            logger.info("MYSQL create table sql: {}", createTableSQL);
            sinkStatement.execute(createTableSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }

    public String convertColumnType(String mysqlType) {
        switch (mysqlType.toLowerCase()) {
            case "date":
            case "datetime":
            case "timestamp":
                return "datetime";
            default:
                return mysqlType;
        }
    }
}
