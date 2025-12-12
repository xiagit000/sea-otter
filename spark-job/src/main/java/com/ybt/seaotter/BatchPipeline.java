package com.ybt.seaotter;

import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.invoker.CallbackInvoker;
import com.ybt.seaotter.source.DataSourceType;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class BatchPipeline {
    Long totalRecords = 0L;
    Long alreadyHandleRecords = 0L;
    String jobId = "";
    Map<String, String> argsMap;

    public BatchPipeline(String[] args) {
        Map<String, String> argsMap = convertArgsToMap(args);
        System.out.println("################ args ################### ");
        for (Map.Entry<String, String> entry : argsMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        this.argsMap = argsMap;
    }

    private void execute() {
        SparkListener listener = new BatchSparkListener(argsMap);
        String source = argsMap.get("source");
        String sink = argsMap.get("sink");
        SparkSession spark = SparkSession.builder()
                .appName(String.format("Batch sync %s to %s", source, sink))
                .config("spark.sql.shuffle.partitions", "10")
                .getOrCreate();
        spark.sparkContext().addSparkListener(listener);
        jobId = argsMap.get("callback.tag");
        new CallbackInvoker(argsMap.get("callback.url")).callback(JobState.START, jobId, totalRecords, alreadyHandleRecords);
        Dataset<Row> mysqlData = getDataset(source, spark);
        totalRecords = mysqlData.count();
        System.out.println("Total records: " + totalRecords);
        // 将数据存储到 StarRocks
        saveToStarRocks(sink, mysqlData, argsMap);
        spark.stop();
    }

    private Dataset<Row> getDataset(String source, SparkSession spark) {
        return DataSourceType.getDataSource(source, argsMap).loadData(spark);
    }

    private static void saveToStarRocks(String sink, Dataset<Row> data, Map<String, String> argsMap) {
        DataSourceType.getDataSource(sink, argsMap).writeData(data);
    }

    private static Map<String, String> convertArgsToMap(String[] args) {
        Map<String, String> argsMap = new HashMap<>();
        for (String arg : args) {
            String[] keyValue = arg.split(" ");
            String key = keyValue[0].replace("--", "");
            if (keyValue.length == 1) {
                argsMap.put(key, "");;
            } else if(keyValue.length == 2) {
                argsMap.put(key, keyValue[1].trim());
            } else {
                StringBuilder value = new StringBuilder();
                for (int i = 1; i < keyValue.length; i++) {
                    value.append(keyValue[i]).append(" ");
                }
                argsMap.put(key, value.toString().trim());
            }
        }
        return argsMap;
    }

    public static void main(String[] args) {
        new BatchPipeline(args).execute();
    }
}
