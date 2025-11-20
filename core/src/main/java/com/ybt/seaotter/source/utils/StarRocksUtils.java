package com.ybt.seaotter.source.utils;

import com.google.common.collect.Lists;
import com.ybt.seaotter.source.impl.db.dm.sql.ColumnRel;
import com.ybt.seaotter.source.impl.db.dm.sql.CreateTable;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StarRocksUtils {

    public static String generateTableCreateSql(CreateTable statement, String tableName,
                                                Function<String, String> convertDataType, int replication_num) {
        List<ColumnRel> keyList = statement.getColumnRels().stream().filter(ColumnRel::getPrimaryKey).collect(Collectors.toList());
        String columnDefinitions = statement.getColumnRels().stream()
                .map(column -> "`" + column.getColumnName() + "` " + convertDataType.apply(column.getTypeName()))
                .collect(Collectors.joining(", "));
        return buildCreateSql(tableName, keyList.stream().map(ColumnRel::getColumnName).collect(Collectors.toList()), columnDefinitions, replication_num);
    }

    private static String buildCreateSql(String tableName, List<String> columns, String columnDefinitions, int replication_num) {
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (")
                .append(columnDefinitions);
        createTableSQL.append(")");
        if (!columns.isEmpty()) {
            createTableSQL.append(" PRIMARY KEY (`");
            createTableSQL.append(String.join("`, `", columns));
            createTableSQL.append("`)");

            createTableSQL.append(" DISTRIBUTED BY HASH (`");
            createTableSQL.append(String.join("`, `", columns));
            createTableSQL.append("`)");

            createTableSQL.append(String.format(" PROPERTIES ('replication_num' = %s)", replication_num));
        }
        return createTableSQL.toString();
    }


    public static String generateTableCreateSql(io.github.melin.superior.common.relational.create.CreateTable statement,
                                                String tableName,
                                                Function<String, String> convertDataType, Integer replicationNum) {
        return generateTableCreateSql(new CreateTable(statement), tableName, convertDataType, replicationNum);
    }

    public static void main(String[] args) {
        System.out.println(buildCreateSql("test", Lists.newArrayList("id", "name", "sex"), "`id` int, `name` varchar, `sex` int", 1));
    }
}
