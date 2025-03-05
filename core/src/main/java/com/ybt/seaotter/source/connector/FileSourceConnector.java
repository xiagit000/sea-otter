package com.ybt.seaotter.source.connector;

import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.meta.file.DirMeta;

public interface FileSourceConnector extends SourceConnector {
    DirMeta getMeta(SeaOtterConfig config);
}
