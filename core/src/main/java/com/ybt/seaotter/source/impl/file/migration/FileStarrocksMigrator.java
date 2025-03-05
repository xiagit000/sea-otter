package com.ybt.seaotter.source.impl.file.migration;

import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.dm.sql.ColumnRel;
import com.ybt.seaotter.source.impl.file.FileConnector;
import com.ybt.seaotter.source.impl.mysql.migration.MysqlStarrocksTableMigrator;
import com.ybt.seaotter.source.impl.starrocks.StarrocksConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class FileStarrocksMigrator implements DataMigrator {

    private FileConnector source;
    private StarrocksConnector sink;
    private Logger logger = LoggerFactory.getLogger(FileStarrocksMigrator.class);

    public FileStarrocksMigrator(FileConnector source, StarrocksConnector sink) {
        this.source = source;
        this.sink = sink;
    }

    private Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(String
                        .format("jdbc:mysql://%s:%s/%s", sink.getHost(), sink.getRpcPort(), sink.getDatabase()),
                sink.getUsername(), sink.getPassword());
    }

    @Override
    public void migrate() {
        List<String> columns = source.getMeta(null).path(source.getPath()).columns();
        String tableName = sink.getTable();
        String columnDefinitions = columns.stream()
                .map(column -> column + " " + "varchar(500)").collect(Collectors.joining(", "));
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (")
                .append(columnDefinitions);
        createTableSQL.append(")");
//        createTableSQL.append(" PRIMARY KEY (");
//        createTableSQL.append(columns.get(0));
//        createTableSQL.append(")");
//
//        createTableSQL.append(" DISTRIBUTED BY HASH (");
//        createTableSQL.append(columns.get(0));
//        createTableSQL.append(")");
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            String createSQL = createTableSQL.toString();
            logger.info("FILE create table sql: {}", createSQL);
            System.out.println(createSQL);
            sinkStatement.execute(createSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }
}
