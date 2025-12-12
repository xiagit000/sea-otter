package com.ybt.seaotter.source.meta;

import com.ybt.seaotter.source.impl.db.dm.sql.CreateTable;

public class Schema {
    private String name;
    private CreateTable statement;

    public Schema(String name, CreateTable statement) {
        this.name = name;
        this.statement = statement;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CreateTable getStatement() {
        return statement;
    }

    public void setStatement(CreateTable statement) {
        this.statement = statement;
    }
}
