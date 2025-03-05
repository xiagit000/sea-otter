package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
import com.ybt.seaotter.deserialization.MyDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlCDCPipeline {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("172.16.2.47")
                .port(3306)
                .databaseList("product_recognition")  // Specify your database(s) to sync
                .tableList("product_recognition.sync_task")  // Specify the tables to sync
                .username("saas_dba")
                .password("@Saas$2023")
                .serverId("5400")
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new MyDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(mysqlSource,
                WatermarkStrategy.noWatermarks(), "flink-cdc-source");

        // Create the data stream from MySQL
        DataStream<StarRocksSinkRowDataWithMeta> mysqlStream = sourceStream.map(new MapFunction<String, StarRocksSinkRowDataWithMeta>() {
            @Override
            public StarRocksSinkRowDataWithMeta map(String record) throws Exception {
                JSONObject jsonRecord = JSON.parseObject(record);
                String database = jsonRecord.getString("database");
                String changeLog = jsonRecord.getString("record");
                String pk = jsonRecord.getString("pk");
                String tableName = jsonRecord.getString("table_name");
                StarRocksSinkRowDataWithMeta rowDataWithMeta = new StarRocksSinkRowDataWithMeta();
                //这里设置下游starrocks 的表名称名称，此示例代码取的是MySQL端的表名称
                rowDataWithMeta.setTable(tableName);
                //这里设置下游starrocks 的库名称，此示例代码取的是MySQL端的库名称
                rowDataWithMeta.setDatabase("data_warehouse");
                /**
                 * 1.获取MYSQL的binlog日志数据，在MysqlDebeziumDeserializationSchema 类里面添加了一个字段 __op字段
                 * 2.__op = 1 代表是删除操作，__op = 0 代表是插入或者更新操作
                 * 3.下游字段取值， changeLog 这个是一个json结构，下游字段名称和json的key匹配的（字段一样的）会写入数据库，不匹配的字段将自动丢弃。
                 */
                System.out.println(changeLog);
                rowDataWithMeta.addDataRow(changeLog);
                return rowDataWithMeta;
            }
        });
        mysqlStream.print();
        mysqlStream.addSink(new StarRocksDynamicSinkFunction<>((new StarRocksSinkOptions.Builder()
                .withProperty("jdbc-url", "jdbc:mysql://172.16.1.51:9030")
                .withProperty("load-url", "172.16.1.51:8080")
                .withProperty("database-name", "data_warehouse")
                .withProperty("table-name", "sync_task")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.max-retries", "3")
                .withProperty("sink.buffer-flush.interval-ms", "3000")
                .withProperty("sink.properties.format", "json")
                .withProperty("table.create.properties.replication_num", "1")
                .build())));

        // Execute the Flink job
        env.execute("Sync MySQL to StarRocks");
    }
}
