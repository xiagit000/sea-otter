package com.ybt.seaotter.source.impl.db.dm.sql;

public class ColumnRel {
    private Boolean primaryKey = false;
    private String columnName;
    private String typeName;

    public ColumnRel() {
    }

    public ColumnRel(Boolean primaryKey, String columnName, String typeName) {
        this.primaryKey = primaryKey;
        this.columnName = columnName;
        this.typeName = typeName;
    }

    public Boolean getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
}
