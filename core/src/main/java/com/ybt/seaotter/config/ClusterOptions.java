package com.ybt.seaotter.config;

public abstract class ClusterOptions {
    private String host;
    private String port;

    public ClusterOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public ClusterOptions setPort(String port) {
        this.port = port;
        return this;
    }
}
