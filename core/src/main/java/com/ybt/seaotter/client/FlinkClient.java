package com.ybt.seaotter.client;

import com.alibaba.fastjson.JSON;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.ApiResponse;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.JarListInfo;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.ybt.seaotter.SeaOtterCDCJob;
import com.ybt.seaotter.SeaOtterJob;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import org.slf4j.Logger;

import java.io.File;

public class FlinkClient {
    private final DBSourceConnector source;
    private final DBSourceConnector target;
    private final FlinkApi api;
    private final String callbackUrl;
    private final String tag;
    private final String JOB_PATH = "flink-job-1.0-SNAPSHOT.jar";
    private String serverId;
    private String jobName;

    private final Logger logger = org.slf4j.LoggerFactory.getLogger(FlinkClient.class);

    public FlinkClient(SeaOtterCDCJob cdcJob) {
        SeaOtterJob job = cdcJob.getSeaOtterJob();
        this.source = (DBSourceConnector) job.getSource();
        this.target = (DBSourceConnector) job.getTarget();
        FlinkApi api = new FlinkApi();
        api.getApiClient().setBasePath(String.format("http://%s:%s", job.getConfig().getFinkOptions().getHost(),
                job.getConfig().getFinkOptions().getPort()));
        this.api = api;
        this.callbackUrl = job.getConfig().getCallbackUrl();
        this.tag = job.getCallbackTag();
        this.serverId = cdcJob.getServerId();
        this.jobName = cdcJob.getJobName();
    }

    public String listJars() {
        try {
            return JSON.toJSONString(api.listJars());
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    public String submit() {
        JarListInfo jars = null;
        try {
            jars = api.listJars();
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        String jarId = jars.getFiles().stream().filter(e -> e.getName().equals(JOB_PATH)).findAny()
                .orElseThrow(() -> new SeaOtterException("flink job not found")).getId();
        System.out.println(JSON.toJSONString(jars));
        StringBuilder argsBuilder = new StringBuilder();
        argsBuilder.append(source.getFlinkArgs().concat(" ").concat(target.getFlinkArgs()));
        argsBuilder.append(String.format(" --callback.url '%s'", callbackUrl));
        argsBuilder.append(String.format(" --callback.tag '%s'", tag));
        argsBuilder.append(String.format(" --flink.serverId '%s'", serverId));
        argsBuilder.append(String.format(" --flink.jobName '%s'", jobName));
        logger.debug("Flink job submit args: {}", argsBuilder);
        JarRunResponseBody response = null;
        try {
//            JarUploadResponseBody result = api.uploadJar(new File("E:\\ybt\\sea-otter\\flink-job\\target\\flink-job-1.0-SNAPSHOT.jar"));
//            String jarId = Arrays.stream(result.getFilename().split("/"))
//                    .reduce((a, b) -> b)
//                    .orElse(null);
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

    public void uploadJar(File file) {
        try {
            api.uploadJar(file);
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
}
