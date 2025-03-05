package com.ybt.seaotter.source.connector;

import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.meta.database.DBMeta;

public interface DBSourceConnector extends SourceConnector {
    DBMeta getMeta(SeaOtterConfig config);
}
