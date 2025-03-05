package com.ybt.seaotter.strategy;

import com.ybt.seaotter.common.SyncMode;

public class SyncStrategy {

    private SyncMode syncMode;
    public SyncStrategy syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    public SyncMode getSyncMode() {
        return syncMode;
    }
}
