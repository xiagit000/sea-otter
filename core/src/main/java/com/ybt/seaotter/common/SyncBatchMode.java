package com.ybt.seaotter.common;

import com.ybt.seaotter.common.enums.TransmissionMode;

public class SyncBatchMode {

    private SyncMode syncMode;

    public SyncBatchMode(SyncMode syncMode) {
        this.syncMode = syncMode;
    }

    public SyncMode fullAddition() {
        syncMode.setTransmissionMode(TransmissionMode.OVERWRITE);
        return syncMode;
    }

    public SyncMode incrementalAddition() {
        syncMode.setTransmissionMode(TransmissionMode.UPSERT);
        return syncMode;
    }
}
