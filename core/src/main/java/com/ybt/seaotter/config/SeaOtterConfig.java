package com.ybt.seaotter.config;

public class SeaOtterConfig {

    private FlinkOptions finkOptions;
    private SparkOptions sparkOptions;
    private String callbackUrl;

    public static SeaOtterConfigBuilder builder() {
        return new SeaOtterConfigBuilder();
    }

    public FlinkOptions getFinkOptions() {
        return finkOptions;
    }

    public void setFinkOptions(FlinkOptions finkOptions) {
        this.finkOptions = finkOptions;
    }

    public SparkOptions getSparkOptions() {
        return sparkOptions;
    }

    public void setSparkOptions(SparkOptions sparkOptions) {
        this.sparkOptions = sparkOptions;
    }

    public void setCallbackUrl(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }
}
