package com.ybt.seaotter.source.meta.database;

import java.util.List;

public interface DatabaseMeta {

    List<String> tables();
    TableMeta table(String structureName);
}
