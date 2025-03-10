package com.ybt.seaotter.source.utils;

import com.ybt.seaotter.source.impl.db.dm.sql.ColumnRel;
import com.ybt.seaotter.source.impl.db.dm.sql.CreateTable;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StarRocksUtils {

    public static String generateTableCreateSql(CreateTable statement, String tableName,
                                                Function<String, String> convertDataType) {
        List<ColumnRel> keyList = statement.getColumnRels().stream().filter(ColumnRel::getPrimaryKey).collect(Collectors.toList());
        String columnDefinitions = statement.getColumnRels().stream()
                .map(column -> "`" + column.getColumnName() + "` " + convertDataType.apply(column.getTypeName()))
                .collect(Collectors.joining(", "));
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (")
                .append(columnDefinitions);
        createTableSQL.append(")");
        if (!keyList.isEmpty()) {
            createTableSQL.append(" PRIMARY KEY (`");
            createTableSQL.append(keyList.stream().map(ColumnRel::getColumnName).collect(Collectors.joining("`, `")));
            createTableSQL.append("`)");

            createTableSQL.append(" DISTRIBUTED BY HASH (`");
            createTableSQL.append(keyList.stream().map(ColumnRel::getColumnName).collect(Collectors.joining("`, `")));
            createTableSQL.append("`)");
        }
        return createTableSQL.toString();
    }

    public static String generateTableCreateSql(io.github.melin.superior.common.relational.create.CreateTable statement,
                                                String tableName,
                                                Function<String, String> convertDataType) {
        return generateTableCreateSql(new CreateTable(statement), tableName, convertDataType);
    }
}
