package com.ybt.seaotter.common;

import com.ybt.seaotter.common.enums.TransmissionMode;

public class SyncMode {
    private static final Integer BATCH_MODE = 1;
    private static final Integer CDC_MODE = 2;
    private Integer mode = BATCH_MODE;
    private TransmissionMode transmissionMode = TransmissionMode.OVERWRITE;

    public static SyncBatchMode batch() {
        SyncMode syncMode = new SyncMode();
        syncMode.mode = BATCH_MODE;
        return new SyncBatchMode(syncMode);
    }

    public static SyncMode batch(TransmissionMode  transmissionMode) {
        SyncMode syncMode = new SyncMode();
        syncMode.mode = BATCH_MODE;
        syncMode.setTransmissionMode(transmissionMode);
        return syncMode;
    }

    public static SyncMode cdc() {
        SyncMode syncMode = new SyncMode();
        syncMode.mode = CDC_MODE;
        return syncMode;
    }

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

    public TransmissionMode getTransmissionMode() {
        return transmissionMode;
    }

    public void setTransmissionMode(TransmissionMode transmissionMode) {
        this.transmissionMode = transmissionMode;
    }
}
