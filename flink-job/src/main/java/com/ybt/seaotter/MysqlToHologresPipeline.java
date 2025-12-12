package com.ybt.seaotter;

import com.alibaba.hologres.com.fasterxml.jackson.core.JsonProcessingException;
import com.alibaba.hologres.com.fasterxml.jackson.databind.DeserializationFeature;
import com.alibaba.hologres.com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.sink.v1.multi.HoloRecordConverter;
import com.starrocks.shade.com.google.common.base.Strings;
import com.ybt.seaotter.common.JobCallback;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.pojo.JobCallbackMessage;
import com.ybt.seaotter.deserialization.MyDebeziumDeserializationSchema;
import com.ybt.seaotter.hologres.CustomHologresMultiTableSinkFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class MysqlToHologresPipeline {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MysqlToHologresPipeline.class);


    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        Map<String, String> argMap = params.toMap();
        logger.info("################ args-111 ################### ");
        for (Map.Entry<String, String> entry : argMap.entrySet()) {
            logger.info("{} : {}", entry.getKey(), entry.getValue());
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.enableCheckpointing(30000);

        Properties properties = new Properties();
        properties.setProperty("snapshot.locking.mode", "none");
        properties.setProperty("scan.incremental.snapshot.enabled", "true");

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(argMap.get("mysql.host"))
                .port(Integer.parseInt(argMap.get("mysql.port")))
                .databaseList(argMap.get("mysql.database"))  // Specify your database(s) to sync
                .tableList(String.format("%s.%s", argMap.get("mysql.database"), argMap.get("mysql.table")))
                .username(argMap.get("mysql.username"))
                .password(argMap.get("mysql.password"))
                .serverId(argMap.get("flink.serverId"))
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new MyDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .heartbeatInterval(Duration.ofMillis(1000))
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(mysqlSource,
                WatermarkStrategy.noWatermarks(), "Mysql cdc source");

        // Create the data stream from MySQL
        DataStream<String> mysqlStream = sourceStream.map(new MapFunction<String, String>() {
            Long alreadyHandleRecords = 0L;
            LocalDateTime lastCallbackTime = LocalDateTime.now();
            final Integer CALLBACK_DURATION = 5; //秒

            @Override
            public String map(String row) throws Exception {
                alreadyHandleRecords += 1;
                CompletableFuture.runAsync(() -> {
                    LocalDateTime now = LocalDateTime.now();
                    if (now.isAfter(lastCallbackTime.plusSeconds(CALLBACK_DURATION))) {
                        lastCallbackTime = now;
                        String callbackUrl = argMap.get("callback.url");
                        if (!Strings.isNullOrEmpty(callbackUrl)) {
                            JobCallbackMessage jobCallbackMessage = new JobCallbackMessage(argMap.get("callback.tag"),
                                    JobState.RUNNING, 0L, alreadyHandleRecords);
                            JobCallback.url(callbackUrl).callback(jobCallbackMessage);
                        }
                    }
                });
                return row;
            }
        });

        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, String.format("%s:%s", argMap.get("hologres.host"), argMap.get("hologres.port")));
        configuration.set(HologresConfigs.DATABASE, argMap.get("hologres.database"));
        configuration.set(HologresConfigs.TABLE, argMap.get("hologres.table"));
        configuration.set(HologresConfigs.USERNAME, argMap.get("hologres.username"));
        configuration.set(HologresConfigs.PASSWORD, argMap.get("hologres.password"));
        // 使用INSERT写入
        configuration.set(HologresConfigs.WRITE_MODE, WriteMode.INSERT);
        // 设置共享连接池的名称以及大小, 所有表共享连接池
        configuration.set(HologresConfigs.CONNECTION_POOL_SIZE, 10);
        configuration.set(HologresConfigs.WRITE_BATCH_SIZE, 2048);
        configuration.set(HologresConfigs.WRITE_BATCH_BYTE_SIZE, 20971520L);
        configuration.set(HologresConfigs.CONNECTION_POOL_NAME, "common-pool");
        // 当json中存在holo表不存在的字段时,配置此参数忽略,否则抛出异常
        configuration.set(HologresConfigs.ENABLE_MULTI_TABLE_EXTRA_COLUMN_TOLERANT, true);
        mysqlStream.addSink(new CustomHologresMultiTableSinkFunction<>(configuration, new RecordConverter()));

        // Execute the Flink job
        env.execute(argMap.get("flink.jobName"));
    }

    public static class RecordConverter implements HoloRecordConverter<String> {
        public Map<String, Object> convert(String record) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
            Map<String, Object> resultMap;
            try {
                resultMap = objectMapper.readValue(record, Map.class);
                resultMap = (Map<String, Object>) resultMap.get("record");
            } catch (JsonProcessingException e) {
                System.out.println("parse json error: " + record);
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return resultMap;
        }
    }

}
