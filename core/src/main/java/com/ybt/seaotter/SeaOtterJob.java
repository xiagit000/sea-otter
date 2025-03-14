package com.ybt.seaotter;

import com.ybt.seaotter.common.enums.TransmissionMode;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.SourceConnector;

public class SeaOtterJob {
    private SourceConnector source;
    private SourceConnector target;
    private final SeaOtterConfig config;
    private String tag;
    private String jobName;

    public SeaOtterJob(SeaOtterConfig config) {
        this.config = config;
    }

    public SeaOtterJob from(SourceConnector source) {
        this.source = source;
        return this;
    }

    public SeaOtterJob to(SourceConnector target) {
        this.target = target;
        return this;
    }

    public void createTable() {
        source.getDataDefine(target).getMigrator().migrate();
    }

    public SeaOtterJob tag(String tag) {
        this.tag = tag;
        return this;
    }

    public SeaOtterCDCJob CDCMode() {
        return new SeaOtterCDCJob(this);
    }

    public SeaOtterBatchJob batchMode(TransmissionMode transmissionMode) {
//        this.strategy = new SyncStrategy().syncMode(SyncMode.batch(transmissionMode));
        return new SeaOtterBatchJob(this, transmissionMode);
    }

    public SeaOtterBatchJob batchMode() {
//        this.strategy = new SyncStrategy().syncMode(SyncMode.batch(transmissionMode));
        return new SeaOtterBatchJob(this, TransmissionMode.OVERWRITE);
    }

    public SourceConnector getSource() {
        return source;
    }

    public SourceConnector getTarget() {
        return target;
    }

    public SeaOtterConfig getConfig() {
        return config;
    }

    public String getCallbackUrl() {
        return config.getCallbackUrl();
    }

    public String getCallbackTag() {
        return tag;
    }

    public String getJobName() {
        return jobName;
    }

    public SeaOtterJob jobName(String jobName) {
        this.jobName = jobName;
        return this;
    }
}
