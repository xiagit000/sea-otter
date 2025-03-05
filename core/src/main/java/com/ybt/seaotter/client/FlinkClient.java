package com.ybt.seaotter.client;

import com.alibaba.fastjson.JSON;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.ApiResponse;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.ybt.seaotter.SeaOtterJob;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.config.FlinkOptions;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.connector.SourceConnector;

import java.io.File;
import java.util.Arrays;

public class FlinkClient {
    private SourceConnector source;
    private SourceConnector target;
    private FlinkApi api;
    private String callbackUrl;
    private String tag;

    public FlinkClient(SeaOtterJob job) {
        this.source = job.getSource();
        this.target = job.getTarget();
        FlinkApi api = new FlinkApi();
        api.getApiClient().setBasePath(String.format("http://%s:%s", job.getConfig().getFinkOptions().getHost(),
                job.getConfig().getFinkOptions().getPort()));
        this.api = api;
        this.callbackUrl = job.getConfig().getCallbackUrl();
        this.tag = job.getCallbackTag();
    }

    public String submit() {
//        JarListInfo jars = api.listJars();
//        System.out.println(JSON.toJSONString(jars));
        StringBuilder argsBuilder = new StringBuilder();
        argsBuilder.append(source.getFlinkArgs().concat(" ").concat(target.getFlinkArgs()));
        argsBuilder.append(String.format(" --callback.url %s", callbackUrl));
        argsBuilder.append(String.format(" --callback.tag %s", tag));

        JarRunResponseBody response = null;
        try {
            JarUploadResponseBody result = api.uploadJar(new File("E:\\ybt\\sea-otter\\flink-job\\target\\flink-job-1.0-SNAPSHOT.jar"));
            String jarId = Arrays.stream(result.getFilename().split("/")).reduce((a, b) -> b).get();
            response = api.runJar(jarId,
                        true, null, argsBuilder.toString(), null,
                        "com.ybt.seaotter.CDCPipeline", null);
        } catch (ApiException e) {
            throw new SeaOtterException("submit flink job failed", e);
        }
        return response.getJobid();
    }

    public JobState detail(String jobId) {
        JobDetailsInfo jobDetailsInfo;
        try {
            jobDetailsInfo = api.getJobDetails(jobId);
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        return toJobState(jobDetailsInfo.getState());
    }

    public JobState toJobState(JobDetailsInfo.StateEnum state) {
        switch (state) {
            case CREATED:
                return JobState.CREATED;
            case RUNNING:
                return JobState.RUNNING;
            case FAILING:
            case FAILED:
                return JobState.FAILED;
            case CANCELLING:
            case CANCELED:
                return JobState.CANCELED;
            case FINISHED:
                return JobState.FINISHED;
            case RESTARTING:
            case SUSPENDING:
            case SUSPENDED:
            case RECONCILING:
            default:
                return JobState.SUSPENDED;
        }
    }

    public Boolean cancel(String jobId) {
        ApiResponse<Void> response;
        try {
            response = api.terminateJobWithHttpInfo(jobId, "cancel");
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        return response.getStatusCode() == 202;
    }
}
