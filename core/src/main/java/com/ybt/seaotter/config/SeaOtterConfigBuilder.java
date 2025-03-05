package com.ybt.seaotter.config;

import com.ybt.seaotter.exceptions.SeaOtterException;

public class SeaOtterConfigBuilder {

    private SeaOtterConfig seaOtterConfig;

    public SeaOtterConfigBuilder() {
        this.seaOtterConfig = new SeaOtterConfig();
    }

    public SeaOtterConfigBuilder sparkOptions(SparkOptions options) {
        this.seaOtterConfig.setSparkOptions(options);
        return this;
    }

    public SeaOtterConfigBuilder flinkOptions(FlinkOptions options) {
        this.seaOtterConfig.setFinkOptions(options);
        return this;
    }

    public SeaOtterConfigBuilder callback(String callbackUrl) {
        this.seaOtterConfig.setCallbackUrl(callbackUrl);
        return this;
    }

    public SeaOtterConfig build() {
        if (this.seaOtterConfig.getSparkOptions() == null) {
            throw new SeaOtterException("spark配置不能为空");
        }
        return this.seaOtterConfig;
    }
}
