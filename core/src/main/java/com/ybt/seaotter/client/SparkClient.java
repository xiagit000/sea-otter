package com.ybt.seaotter.client;

import com.alibaba.fastjson.JSON;
import com.github.ywilkof.sparkrestclient.DriverState;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.JobStatusResponse;
import com.github.ywilkof.sparkrestclient.SparkRestClient;
import com.google.common.collect.Lists;
import com.ybt.seaotter.SeaOtterBatchJob;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.config.SparkOptions;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.connector.SourceConnector;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

public class SparkClient {
    private SourceConnector source;
    private SourceConnector target;
    private final SparkOptions sparkOptions;
    private String upsertColumn;
    private String columnVal;
    private String callbackUrl;
    private String callbackTag;
    private final SparkRestClient sparkRestClient;
    private final static String SPARK_JARS_PATH = "/opt/jars";
    private final Logger logger = org.slf4j.LoggerFactory.getLogger(SparkClient.class);
    private final SeaOtterBatchJob seaOtterBatchJob;

    public SparkClient(SeaOtterBatchJob  seaOtterBatchJob) {
        this.seaOtterBatchJob = seaOtterBatchJob;
        this.sparkOptions = seaOtterBatchJob.getSeaOtterSync().getConfig().getSparkOptions();
        this.sparkRestClient = SparkRestClient.builder()
                .masterHost(sparkOptions.getHost())
                .masterPort(sparkOptions.getPort())
                .sparkVersion(sparkOptions.getVersion())
                .build();
    }

    public void init() {
        this.source = seaOtterBatchJob.getSeaOtterSync().getSource();
        this.target = seaOtterBatchJob.getSeaOtterSync().getTarget();
        this.upsertColumn = seaOtterBatchJob.getUpsertColumn();
        this.columnVal = seaOtterBatchJob.getColumnVal();
        this.callbackUrl = seaOtterBatchJob.getSeaOtterSync().getCallbackUrl();
        this.callbackTag = seaOtterBatchJob.getSeaOtterSync().getCallbackTag();
    }

    public String submit() {
        init();
        final String submissionId;
        try {
            List<String> args = Lists.newArrayList();
            args.addAll(Arrays.asList(source.getSparkArgs()));
            args.addAll(Arrays.asList(target.getSparkArgs()));
            if (upsertColumn != null && columnVal != null) {
                args.add(String.format("--upsertColumn %s", upsertColumn));
                args.add(String.format("--columnVal %s", columnVal));
            }
            args.add(String.format("--callback.url %s", callbackUrl));
            args.add(String.format("--callback.tag %s", callbackTag));
            args.add(String.format("--source %s", source.getName()));
            logger.debug("Spark job submit args: {}", JSON.toJSONString(args));
            String jarPath = source.getDataDefine(target).getDriverJar();
            String driverJarPath = jarPath == null ? "" : String.format("file://%s/%s,", SPARK_JARS_PATH, jarPath);
            submissionId = sparkRestClient.prepareJobSubmit()
                    .appName(sparkOptions.getAppName())
                    .appResource(String.format("file://%s/spark-job-1.0-SNAPSHOT.jar", SPARK_JARS_PATH))
                    .mainClass("com.ybt.seaotter.BatchPipeline")
                    .appArgs(args)
                    .withProperties()
                    .put("spark.driver.memory", "1g")
                    .put("spark.driver.memoryOverhead", "2g")
                    .put("spark.driver.cores", "1")
                    .put("spark.executor.memory", "4g")
                    .put("spark.executor.memoryOverhead", "1g")
                    .put("spark.executor.instances", "1")
                    .put("spark.executor.cores", "1")
                    .put("spark.jars",
                            driverJarPath +
                                    "file://" + SPARK_JARS_PATH + "/mysql-connector-java-8.0.28.jar," +
                                    "file://" + SPARK_JARS_PATH + "/starrocks-spark-connector-3.4_2.12-1.1.2.jar," +
                                    "file://" + SPARK_JARS_PATH + "/commons-net-3.8.0.jar," +
                                    "file://" + SPARK_JARS_PATH + "/jsch-0.1.55.jar," +
                                    "file://" + SPARK_JARS_PATH + "/fastjson-1.2.83.jar"
                    )
                    .put("spark.driver.extraClassPath", driverJarPath + "file://" + SPARK_JARS_PATH + "/mysql-connector-java-8.0.28.jar")
                    .put("spark.executor.extraClassPath", driverJarPath + "file://" + SPARK_JARS_PATH + "/mysql-connector-java-8.0.28.jar")
                    .put("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                    .put("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                    .submit();
        } catch (FailedSparkRequestException e) {
            throw new SeaOtterException("submit spark job failed", e);
        }
        return submissionId;
    }

    public JobState detail(String jobId) {
        JobStatusResponse jobStatusResponse;
        try {
            jobStatusResponse = sparkRestClient.checkJobStatus().withSubmissionIdFullResponse(jobId);
        } catch (FailedSparkRequestException e) {
            throw new RuntimeException(e);
        }
        return toJobState(jobStatusResponse.getDriverState());
    }

    private JobState toJobState(DriverState driverState) {
        switch (driverState) {
            case SUBMITTED:
            case QUEUED:
            case RETRYING:
                return JobState.CREATED;
            case RUNNING:
                return JobState.RUNNING;
            case FINISHED:
                return JobState.FINISHED;
            case KILLED:
                return JobState.CANCELED;
            case FAILED:
                return JobState.FAILED;
            case RELAUNCHING:
            case UNKNOWN:
            case ERROR:
            case NOT_FOUND:
            default:
                return JobState.SUSPENDED;
        }
    }

    public Boolean cancel(String jobId) {
        boolean cancelResult;
        try {
            cancelResult = sparkRestClient.killJob().withSubmissionId(jobId);
        } catch (FailedSparkRequestException e) {
            throw new RuntimeException(e);
        }
        return cancelResult;
    }
}
