package com.ybt.seaotter;

import com.github.ywilkof.sparkrestclient.JobStatusResponse;
import com.google.common.base.Strings;
import com.ybt.seaotter.client.SparkClient;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.enums.TransmissionMode;

public class SeaOtterBatchJob {
    private final SeaOtterJob seaOtterSync;
    private final SparkClient sparkClient;

    private String upsertColumn;
    private String columnVal;
    private TransmissionMode  transmissionMode = TransmissionMode.OVERWRITE;
    private String jobName;

    public SeaOtterBatchJob(SeaOtterJob seaOtterJob) {
        if (!Strings.isNullOrEmpty(seaOtterJob.getJobName())) {
            this.jobName = seaOtterJob.getJobName();
        }
        this.seaOtterSync = seaOtterJob;
        this.sparkClient = new SparkClient(this);
    }

    public SeaOtterBatchJob(SeaOtterJob seaOtterSync, TransmissionMode transmissionMode) {
        this.seaOtterSync = seaOtterSync;
        this.transmissionMode = transmissionMode;
        this.sparkClient = new SparkClient(this);
    }

    public String submit() {
        return sparkClient.submit();
    }

    public JobState detail(String jobId) {
        return sparkClient.detail(jobId);
    }

    public Boolean cancel(String jobId) {
        return sparkClient.cancel(jobId);
    }



    public SeaOtterBatchJob filter(String upsertColumn, String val) {
        if (this.transmissionMode == TransmissionMode.OVERWRITE) {
            return this;
        }
        this.upsertColumn = upsertColumn;
        this.columnVal = val;
        return this;
    }

    public SeaOtterJob getSeaOtterSync() {
        return seaOtterSync;
    }

    public String getUpsertColumn() {
        return upsertColumn;
    }

    public String getColumnVal() {
        return columnVal;
    }

    public TransmissionMode getTransmissionMode() {
        return transmissionMode;
    }

    public String getJobName() {
        return jobName;
    }
}
