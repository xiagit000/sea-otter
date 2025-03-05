package com.ybt.seaotter.source.impl.dm.sql;

import java.util.List;

public class CreateTable {
    private String tableName;
    private List<ColumnRel> columnRels;

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
