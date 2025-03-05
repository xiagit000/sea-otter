package com.ybt.seaotter.common;

import com.alibaba.fastjson.JSON;
import com.ybt.seaotter.common.http.HttpExecutor;
import com.ybt.seaotter.common.pojo.JobCallbackMessage;

public class JobCallback {

    private String callbackUrl;

    public JobCallback(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }

    public static JobCallback url(String url) {
        return new JobCallback(url);
    }

    public void callback(JobCallbackMessage message) {
        HttpExecutor.post(callbackUrl, JSON.toJSONString(message));
    }
}
