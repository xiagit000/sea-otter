package com.ybt.seaotter.source.meta.database;

import java.util.List;

public interface DBMeta {

    List<String> databases();
    DatabaseMeta database(String containerName);
}
