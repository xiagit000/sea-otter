package com.ybt.seaotter;

import com.ybt.seaotter.common.JobCallback;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.pojo.JobCallbackMessage;
import com.ybt.seaotter.utils.DownloadUtils;
import org.apache.parquet.Strings;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class BatchPipeline {
    Long totalRecords = 0L;
    Long alreadyHandleRecords = 0L;
    String jobId = "";
    Map<String, String> argsMap;
    boolean success = false;
    String reason;

    public BatchPipeline(String[] args) {
        Map<String, String> argsMap = convertArgsToMap(args);
        System.out.println("################ args ################### ");
        for (Map.Entry<String, String> entry : argsMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        this.argsMap = argsMap;
    }

    private void execute() {
        SparkListener listener = new SparkListener() {



            //            @Override
//            public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
//                long inputRecords = stageCompleted.stageInfo().taskMetrics().inputMetrics().recordsRead();
//                long outputRecords = stageCompleted.stageInfo().taskMetrics().outputMetrics().recordsWritten();
//                System.out.println("Stage Completed: Processed " + inputRecords + " input records and " + outputRecords + " output records.");
//            }

            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
                if (taskEnd.taskMetrics() == null) return;
                long inputRecords = taskEnd.taskMetrics().inputMetrics().recordsRead();
                long outputRecords = taskEnd.taskMetrics().outputMetrics().recordsWritten();
                System.out.println("Task Completed: Processed " + inputRecords + " input records and " + outputRecords + " output records.");
                if (totalRecords > 0) {
                    alreadyHandleRecords += inputRecords;
                }
                callback(JobState.RUNNING);
                success = true;
            }


            @Override
            public void onJobEnd(SparkListenerJobEnd jobEnd) {
                if (jobEnd.jobResult() instanceof JobFailed) {
                    reason = ((JobFailed) jobEnd.jobResult()).exception().getMessage();
                    callback(JobState.FAILED, reason);
                    success = false;
                }
            }

            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
                if (success) {
                    System.out.println("application " + jobId + " completed successfully.");
                    callback(JobState.SUCCESS);
                } else {
                    System.out.println("application " + jobId + " failed. Reason: " + reason);
                    callback(JobState.FAILED, reason);
                }

            }
        };
        String source = argsMap.get("source");
        SparkSession spark = SparkSession.builder()
                .appName(String.format("Batch sync %s to Starrocks", source))
                .config("spark.sql.shuffle.partitions", "10")
                .getOrCreate();

        spark.sparkContext().addSparkListener(listener);
        jobId = argsMap.get("callback.tag");
        callback(JobState.START);
        Dataset<Row> mysqlData = getDataset(source, spark);
        totalRecords = mysqlData.count();
        System.out.println("Total records: " + totalRecords);

        // 将数据存储到 StarRocks
        saveToStarRocks(mysqlData, argsMap);
        spark.stop();
    }

    private Dataset<Row> getDataset(String source, SparkSession spark) {
        if (source.equals("MYSQL")) {
            DataFrameReader reader = spark.read()
                    .format("jdbc")
                    .option("url", String.format("jdbc:mysql://%s:%s/%s", argsMap.get("mysql.host"),
                            argsMap.get("mysql.port"), argsMap.get("mysql.database")))
                    .option("dbtable", argsMap.get("mysql.table"))
                    .option("user", argsMap.get("mysql.username"))
                    .option("password", argsMap.get("mysql.password"));
            if (argsMap.get("upsertColumn") != null && !argsMap.get("upsertColumn").isEmpty()) {
                reader.option("where", String.format("%s >= %s", argsMap.get("upsertColumn"), argsMap.get("columnVal")));
            }
            return reader.load();
        } else if (source.equals("DM")) {
            DataFrameReader reader = spark.read()
                    .format("jdbc")
                    .option("url", String.format("jdbc:dm://%s:%s/%s", argsMap.get("dm.host"),
                            argsMap.get("dm.port"), argsMap.get("dm.database")))
                    .option("dbtable", argsMap.get("dm.table"))
                    .option("user", argsMap.get("dm.username"))
                    .option("password", argsMap.get("dm.password"));
            if (argsMap.get("upsertColumn") != null && !argsMap.get("upsertColumn").isEmpty()) {
                reader.option("where", String.format("%s >= %s", argsMap.get("upsertColumn"), argsMap.get("columnVal")));
            }
            return reader.load();
        } else if (source.equals("ORACLE")) {
            DataFrameReader reader = spark.read()
                    .format("jdbc")
                    .option("url", String.format("jdbc:oracle:thin:@%s:%s:%s", argsMap.get("oracle.host"),
                            argsMap.get("oracle.port"), argsMap.get("oracle.sid")))
                    .option("dbtable", String.format("%s.%s", argsMap.get("oracle.database"), argsMap.get("oracle.table")))
                    .option("user", argsMap.get("oracle.username"))
                    .option("password", argsMap.get("oracle.password"))
                    .option("driver", "oracle.jdbc.OracleDriver");
            if (argsMap.get("upsertColumn") != null && !argsMap.get("upsertColumn").isEmpty()) {
                reader.option("where", String.format("%s >= %s", argsMap.get("upsertColumn"), argsMap.get("columnVal")));
            }
            return reader.load();
        } else if (source.equals("FTP")) {
            String fileName = argsMap.get("ftp.path").substring(argsMap.get("ftp.path").lastIndexOf("/") + 1);
            String localFilePath = "/opt/bitnami/spark/tmp/download/".concat(argsMap.get("callback.tag")).concat("_").concat(fileName);
            DownloadUtils.downloadByFtp(argsMap.get("ftp.host"), Integer.parseInt(argsMap.get("ftp.port")),
                    argsMap.get("ftp.username"), argsMap.get("ftp.password"), argsMap.get("ftp.path"), localFilePath);
            return spark.read().option("header", "true").option("delimiter", argsMap.get("ftp.separator")).csv(localFilePath);
        } else if (source.equals("SFTP")) {
            String fileName = argsMap.get("sftp.path").substring(argsMap.get("sftp.path").lastIndexOf("/") + 1);
            String localFilePath = "/opt/bitnami/spark/tmp/download/".concat(argsMap.get("callback.tag")).concat("_").concat(fileName);
            DownloadUtils.downloadBySftp(argsMap.get("sftp.host"), Integer.parseInt(argsMap.get("sftp.port")),
                    argsMap.get("sftp.username"), argsMap.get("sftp.password"), argsMap.get("sftp.path"), localFilePath);
            return spark.read().option("header", "true").option("delimiter", argsMap.get("ftp.separator")).csv(localFilePath);
        } else {
            throw new RuntimeException("source options happened error");
        }
    }

    private void callback(Integer state) {
        callback(state, null);
    }

    private void callback(Integer state, String message) {
        CompletableFuture.runAsync(() -> {
            String callbackUrl = argsMap.get("callback.url");
            if (Strings.isNullOrEmpty(callbackUrl)) {
                return;
            }
            JobCallbackMessage jobCallbackMessage = new JobCallbackMessage(jobId, state, totalRecords, alreadyHandleRecords);
            jobCallbackMessage.setMessage(message);
            JobCallback.url(callbackUrl).callback(jobCallbackMessage);
        });
    }

    private static void saveToStarRocks(Dataset<Row> data, Map<String, String> argsMap) {
        data.write()
                .format("starrocks")
                .option("sink.buffer-flush.max-rows", "10000")
                .option("sink.buffer-flush.max-bytes", "2097152")
                .option("starrocks.fe.http.url", String.format("%s:%s", argsMap.get("starrocks.host"),
                        argsMap.get("starrocks.httpPort")))
                .option("starrocks.fe.jdbc.url", String.format("jdbc:mysql://%s:%s", argsMap.get("starrocks.host"),
                        argsMap.get("starrocks.rpcPort")))
                .option("starrocks.table.identifier", argsMap.get("starrocks.database").concat(".")
                        .concat(argsMap.get("starrocks.table")))
                .option("starrocks.user", argsMap.get("starrocks.username"))
                .option("starrocks.password", argsMap.get("starrocks.password"))
                .mode(SaveMode.Append)
                .save();
    }

    private static Map<String, String> convertArgsToMap(String[] args) {
        Map<String, String> argsMap = new HashMap<>();
        for (String arg : args) {
            String[] keyValue = arg.split(" ");
            String key = keyValue[0].replace("--", "");
            if (keyValue.length == 1) {
                argsMap.put(key, "");;
            } else if(keyValue.length == 2) {
                argsMap.put(key, keyValue[1].trim());
            } else {
                StringBuilder value = new StringBuilder();
                for (int i = 1; i < keyValue.length; i++) {
                    value.append(keyValue[i]).append(" ");
                }
                argsMap.put(key, value.toString().trim());
            }
        }
        return argsMap;
    }

    public static void main(String[] args) {
        new BatchPipeline(args).execute();
    }
}
