package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

public class BatchSparkListener extends SparkListener {

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        System.out.println("Job " + applicationStart.appId() + " started.");
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        System.out.println("Job ended.");
    }
}
