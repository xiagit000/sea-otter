package com.ybt.seaotter.source.impl.file.sftp.migration;

import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.ddl.DataMigrator;
import com.ybt.seaotter.source.impl.db.dm.sql.ColumnRel;
import com.ybt.seaotter.source.impl.db.dm.sql.CreateTable;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.impl.file.sftp.SftpConnector;
import com.ybt.seaotter.source.utils.StarRocksUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class SftpStarrocksMigrator implements DataMigrator {
    private final SftpConnector source;
    private final StarrocksConnector sink;
    private final Logger logger = LoggerFactory.getLogger(SftpStarrocksMigrator.class);

    public SftpStarrocksMigrator(SftpConnector source, StarrocksConnector sink) {
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
        CreateTable statement = new CreateTable();
        List<String> columns = source.getMeta(null).path(source.getPath()).columns();
        List<ColumnRel> columnRels = columns.stream().map(column ->
                new ColumnRel(false, column, "varchar(500)")).collect(Collectors.toList());
        statement.setColumnRels(columnRels);
        String createTableSQL = StarRocksUtils.generateTableCreateSql(statement, sink.getTable(), s -> s);
        try (Connection sinkConnection =  getSinkConnection();
             Statement sinkStatement = sinkConnection.createStatement()) {
            logger.info("SFTP create table sql: {}", createTableSQL);
            System.out.println(createTableSQL);
            sinkStatement.execute(createTableSQL);
        }  catch (SQLException e) {
            throw new SeaOtterException(e.getMessage());
        }
    }
}
