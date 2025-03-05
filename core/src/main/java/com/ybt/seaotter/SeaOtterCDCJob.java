package com.ybt.seaotter;

import com.nextbreakpoint.flinkclient.api.ApiResponse;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.ybt.seaotter.client.FlinkClient;
import com.ybt.seaotter.common.enums.JobState;

public class SeaOtterCDCJob {
    private FlinkClient cdcClient;

    public SeaOtterCDCJob(SeaOtterJob seaOtterSync) {
        this.cdcClient = new FlinkClient(seaOtterSync);
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
}
