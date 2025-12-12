package com.ybt.seaotter.hologres;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.v1.AbstractHologresOutputFormat;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHologresMapOutputFormat extends AbstractHologresOutputFormat<Map<String, Object>> {

    private static final transient Logger LOG = LoggerFactory.getLogger(CustomHologresMapOutputFormat.class);

    public CustomHologresMapOutputFormat(HologresConnectionParam param, HologresWriter<Map<String, Object>> hologresIOClient) {
        super(param, hologresIOClient);
    }

    public long writeData(Map<String, Object> record) throws HoloClientException, IOException {
        return this.hologresIOClient.writeAddRecord(record);
    }

    public long deleteData(Map<String, Object> record) throws HoloClientException, IOException {
        return this.hologresIOClient.writeDeleteRecord(record);
    }
}
