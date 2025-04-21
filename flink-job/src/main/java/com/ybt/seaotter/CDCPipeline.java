package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
import com.starrocks.shade.com.google.common.base.Strings;
import com.ybt.seaotter.common.JobCallback;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.pojo.JobCallbackMessage;
import com.ybt.seaotter.deserialization.MyDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class CDCPipeline {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(CDCPipeline.class);


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
                WatermarkStrategy.noWatermarks(), "flink-cdc-source");

        // Create the data stream from MySQL
        DataStream<StarRocksSinkRowDataWithMeta> mysqlStream = sourceStream.map(new MapFunction<String, StarRocksSinkRowDataWithMeta>() {
            Long alreadyHandleRecords = 0L;
            LocalDateTime lastCallbackTime = LocalDateTime.now();
            final Integer CALLBACK_DURATION = 5; //秒

            @Override
            public StarRocksSinkRowDataWithMeta map(String record) throws Exception {
                JSONObject jsonRecord = JSON.parseObject(record);
                String database = jsonRecord.getString("database");
                String changeLog = jsonRecord.getString("record");
                String pk = jsonRecord.getString("pk");
                String tableName = jsonRecord.getString("table_name");
                StarRocksSinkRowDataWithMeta rowDataWithMeta = new StarRocksSinkRowDataWithMeta();
                rowDataWithMeta.setTable(argMap.get("starrocks.table"));
                rowDataWithMeta.setDatabase(argMap.get("starrocks.database"));
                /**
                 * 1.获取MYSQL的binlog日志数据，在MysqlDebeziumDeserializationSchema 类里面添加了一个字段 __op字段
                 * 2.__op = 1 代表是删除操作，__op = 0 代表是插入或者更新操作
                 * 3.下游字段取值， changeLog 这个是一个json结构，下游字段名称和json的key匹配的（字段一样的）会写入数据库，不匹配的字段将自动丢弃。
                 */
//                System.out.println("changLog: " + changeLog);
                rowDataWithMeta.addDataRow(changeLog);
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
                return rowDataWithMeta;
            }
        });
//        mysqlStream.print();
        mysqlStream.addSink(new StarRocksDynamicSinkFunction<>((new StarRocksSinkOptions.Builder()
                .withProperty("jdbc-url", String.format("jdbc:mysql://%s:%s", argMap.get("starrocks.host"),
                        argMap.get("starrocks.rpcPort")))
                .withProperty("load-url", String.format("%s:%s", argMap.get("starrocks.host"), argMap.get("starrocks.httpPort")))
                .withProperty("database-name", argMap.get("starrocks.database"))
                .withProperty("table-name", argMap.get("starrocks.table"))
                .withProperty("username", argMap.get("starrocks.username"))
                .withProperty("password", argMap.get("starrocks.password").equals("__NO_VALUE_KEY") ? "" : argMap.get("starrocks.password"))
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.max-retries", "3")
                .withProperty("sink.buffer-flush.interval-ms", "3000")
                .withProperty("sink.properties.format", "json")
                .withProperty("table.create.properties.replication_num", "1")
                .build())));

        // Execute the Flink job
        env.execute(argMap.get("flink.jobName"));
    }

}
