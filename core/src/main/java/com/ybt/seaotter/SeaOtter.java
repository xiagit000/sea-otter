package com.ybt.seaotter;

import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.meta.database.DBMeta;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.meta.file.DirMeta;

public class SeaOtter {

    private final SeaOtterConfig config;


    public SeaOtter(SeaOtterConfig config) {
        this.config = config;
    }

    public static SeaOtter config(SeaOtterConfig config) {
        return new SeaOtter(config);
    }

    public SeaOtterJob job() {
        return new SeaOtterJob(config);
    }

    public DBMeta db(SourceConnector sourceConnector) {
        return new SeaOtterQuery(config, sourceConnector).db();
    }

    public DirMeta file(SourceConnector sourceConnector) {
        return new SeaOtterQuery(config, sourceConnector).file();
    }
}
