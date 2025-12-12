package com.ybt.seaotter.invoker;

import com.ybt.seaotter.common.JobCallback;
import com.ybt.seaotter.common.pojo.JobCallbackMessage;
import org.apache.parquet.Strings;

import java.util.concurrent.CompletableFuture;

public class CallbackInvoker {
    String url;

    public CallbackInvoker(String url) {
        this.url  = url;
    }

    public void callback(Integer state, String jobId, Long totalRecords, Long alreadyHandleRecords) {
        callback(state, null, jobId, totalRecords, alreadyHandleRecords);
    }

    public void callback(Integer state, String message, String jobId, Long totalRecords, Long alreadyHandleRecords) {
        CompletableFuture.runAsync(() -> {
            String callbackUrl = url;
            if (Strings.isNullOrEmpty(callbackUrl)) {
                return;
            }
            JobCallbackMessage jobCallbackMessage = new JobCallbackMessage(jobId, state, totalRecords, alreadyHandleRecords);
            jobCallbackMessage.setMessage(message);
            JobCallback.url(callbackUrl).callback(jobCallbackMessage);
        });
    }
}
