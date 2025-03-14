package com.ybt.seaotter;

import com.google.common.base.Strings;
import com.nextbreakpoint.flinkclient.api.ApiResponse;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.ybt.seaotter.client.FlinkClient;
import com.ybt.seaotter.common.enums.JobState;

import java.io.File;

public class SeaOtterCDCJob {
    private final FlinkClient cdcClient;
    private final SeaOtterJob seaOtterJob;
    private String serverId = "5400";
    private String jobName = "Sync MySQL to StarRocks";

    public SeaOtterCDCJob(SeaOtterJob seaOtterJob) {
        if (!Strings.isNullOrEmpty(seaOtterJob.getJobName())) {
            this.jobName = seaOtterJob.getJobName();
        }
        this.seaOtterJob = seaOtterJob;
        this.cdcClient = new FlinkClient(this);
    }

    public String getServerId() {
        return serverId;
    }

    public String getJobName() {
        return jobName;
    }

    public SeaOtterJob getSeaOtterJob() {
        return seaOtterJob;
    }

    public String submit() {
        return cdcClient.submit();
    }

    public JobState detail(String jobId) {
        return cdcClient.detail(jobId);
    }

    public Boolean cancel(String jobId) {
        return cdcClient.cancel(jobId);
    }

    public String listJars() {
        return cdcClient.listJars();
    }

    public void uploadJar(File file) {
        cdcClient.uploadJar(file);
    }

    public SeaOtterCDCJob serverId(String serverId) {
        this.serverId = serverId;
        this.cdcClient.setServerId(serverId);
        return this;
    }
}
