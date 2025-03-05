package com.ybt.seaotter.source.impl.dm.sql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DmSqlHelper {

    public static CreateTable parseCreateTableStatement(String sql) {
        CreateTable createTable = new CreateTable();

        Pattern tablePattern = Pattern.compile("CREATE\\s+TABLE\\s+\"(\\w+)\"\\.\"(\\w+)\"", Pattern.CASE_INSENSITIVE);
        Matcher tableMatcher = tablePattern.matcher(sql);
        if (tableMatcher.find()) {
            String schemaName = tableMatcher.group(1);
            String tableName = tableMatcher.group(2);
            createTable.setTableName(tableName);
        }

        Set<String> primaryKeys = new HashSet<>();
        Pattern pkPattern = Pattern.compile("PRIMARY KEY\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher pkMatcher = pkPattern.matcher(sql);
        if (pkMatcher.find()) {
            String pkFields = pkMatcher.group(1);
            // 处理多个主键字段
            for (String pk : pkFields.split(",")) {
                primaryKeys.add(pk.replaceAll("[\"\\s]", "")); // 去掉双引号和空格
            }
        }

        Pattern columnPattern = Pattern.compile("\"(\\w+)\"\\s+([A-Z]+(?:\\(\\d+(?:,\\d+)?\\))?)", Pattern.CASE_INSENSITIVE);
        Matcher columnMatcher = columnPattern.matcher(sql);

        List<ColumnRel> columns = new ArrayList<>();
        while (columnMatcher.find()) {
            ColumnRel column = new ColumnRel();
            String columnName = columnMatcher.group(1);
            String columnType = columnMatcher.group(2);
            boolean isPrimaryKey = primaryKeys.contains(columnName);
            column.setColumnName(columnName);
            column.setTypeName(columnType);
            column.setPrimaryKey(isPrimaryKey);
            columns.add(column);
        }
        createTable.setColumnRels(columns);
        return createTable;
    }
}
