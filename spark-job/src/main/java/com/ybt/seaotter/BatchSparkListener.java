package com.ybt.seaotter;

import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.invoker.CallbackInvoker;
import org.apache.spark.scheduler.*;

import java.util.Map;

public class BatchSparkListener extends SparkListener {

    Long totalRecords = 0L;
    Long alreadyHandleRecords = 0L;
    Map<String, String> argsMap;
    boolean success = false;
    String reason;
    String jobId = "";

    public BatchSparkListener(Map<String, String> argsMap) {
        this.argsMap = argsMap;
    }

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
        new CallbackInvoker(argsMap.get("callback.url")).callback(JobState.RUNNING, jobId, totalRecords, alreadyHandleRecords);
        success = true;
    }


    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        if (jobEnd.jobResult() instanceof JobFailed) {
            reason = ((JobFailed) jobEnd.jobResult()).exception().getMessage();
            new CallbackInvoker(argsMap.get("callback.url")).callback(JobState.FAILED, reason, jobId, totalRecords, alreadyHandleRecords);
            success = false;
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        if (success) {
            System.out.println("application " + jobId + " completed successfully.");
            new CallbackInvoker(argsMap.get("callback.url")).callback(JobState.SUCCESS, jobId, totalRecords, alreadyHandleRecords);
        } else {
            System.out.println("application " + jobId + " failed. Reason: " + reason);
            new CallbackInvoker(argsMap.get("callback.url")).callback(JobState.FAILED, reason, jobId, totalRecords, alreadyHandleRecords);
        }

    }
}
