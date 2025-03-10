package com.ybt.seaotter.source.impl.db.dm.sql;

import java.util.List;
import java.util.stream.Collectors;

public class CreateTable {
    private String tableName;
    private List<ColumnRel> columnRels;

    public CreateTable() {
    }

    public CreateTable(String tableName, io.github.melin.superior.common.relational.create.CreateTable createTable) {
        this.tableName = tableName;
        this.columnRels = createTable.getColumnRels().stream().map(column -> {
            ColumnRel columnRel = new ColumnRel();
            columnRel.setPrimaryKey(column.getPrimaryKey());
            columnRel.setTypeName(column.getTypeName());
            columnRel.setColumnName(column.getColumnName());
            return columnRel;
        }).collect(Collectors.toList());
    }

    public CreateTable(io.github.melin.superior.common.relational.create.CreateTable createTable) {
        this.columnRels = createTable.getColumnRels().stream().map(column -> {
            ColumnRel columnRel = new ColumnRel();
            columnRel.setPrimaryKey(column.getPrimaryKey());
            columnRel.setTypeName(column.getTypeName());
            columnRel.setColumnName(column.getColumnName());
            return columnRel;
        }).collect(Collectors.toList());
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnRel> getColumnRels() {
        return columnRels;
    }

    public void setColumnRels(List<ColumnRel> columnRels) {
        this.columnRels = columnRels;
    }
}
