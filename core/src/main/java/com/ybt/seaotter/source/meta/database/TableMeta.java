package com.ybt.seaotter.source.meta.database;

import java.util.List;

public interface TableMeta {
    List<String> columns();
    List<List<String>> rows(Integer limit);
}
