package com.ybt.seaotter.config;

public class SparkOptions {
    private String host;
    private Integer port;
    private String appName;
    private String version = "3.4.3";

    public SparkOptions(String appName, String host, Integer port) {
        this.host = host;
        this.appName = appName;
        this.port = port;
    }

    public SparkOptions(String host, Integer port) {
        this.host = host;
        this.port = port;
        this.appName = "spark-app";
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
