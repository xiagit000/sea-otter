package com.ybt.seaotter;

import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.FileSourceConnector;
import com.ybt.seaotter.source.meta.database.DBMeta;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.meta.file.DirMeta;

public class SeaOtterQuery {

    private SeaOtterConfig config;
    private SourceConnector connector;

    public SeaOtterQuery(SeaOtterConfig config, SourceConnector sourceConnector) {
        this.config = config;
        this.connector = sourceConnector;
    }

    public DBMeta db() {
        return ((DBSourceConnector) connector).getMeta(config);
    }

    public DirMeta file() {
        return ((FileSourceConnector) connector).getMeta(config);
    }
}
