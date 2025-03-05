package com.ybt.seaotter;

public class FlinkSqlCDC {
    public static void main(String[] args) throws Exception {
        // 设置 Flink 流处理环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.getConfig().getConfiguration().setString("table.planner", "blink");
//
//
//        // 2. 注册 MySQL CDC Source 表
//        String createMySQLSource = "CREATE TABLE IF NOT EXISTS sync_task_source (\n" +
//                "  id INT,\n" +
//                "  task_no STRING,\n" +
//                "  PRIMARY KEY (id) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "  'connector' = 'mysql-cdc',\n" +
//                "  'hostname' = '172.16.2.47',\n" +
//                "  'port' = '3306',\n" +
//                "  'username' = 'saas_dba',\n" +
//                "  'password' = '@Saas$2023',\n" +
//                "  'database-name' = 'product_recognition',\n" +
//                "  'table-name' = 'sync_task'\n" +
//                ")";
//        tEnv.executeSql(createMySQLSource);
//
//        // 3. 注册 StarRocks Sink 表
//        String createStarRocksSink = "CREATE TABLE IF NOT EXISTS `sync_task_target` (\n" +
//                "  `id` INT,\n" +
//                "  `task_no` STRING,\n" +
//                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "  'connector' = 'starrocks',\n" +
//                "  'jdbc-url' = 'jdbc:mysql://172.16.1.51:9030',\n" +
//                "  'load-url' = '172.16.1.51:8080',\n" +
//                "  'database-name' = 'data_warehouse',\n" +
//                "  'table-name' = 'sync_task',\n" +
//                "  'username' = 'root',\n" +
//                "  'password' = '',\n" +
//                "  'sink.properties.strip_outer_array' = 'true',\n" +
//                "  'sink.max-retries' = '10',\n" +
//                "  'sink.buffer-flush.interval-ms' = '15000',\n" +
//                "  'sink.properties.format' = 'json'\n" +
//                ")";
//        tEnv.executeSql(createStarRocksSink);
//
//        // 4. 使用 SQL 插入数据从 MySQL 到 StarRocks
//        String insertSQL = "INSERT INTO sync_task_target SELECT id, task_no FROM sync_task_source";
//        tEnv.executeSql(insertSQL);
//
//        // 5. 执行作业
//        env.execute("MySQL to StarRocks SQL Sync Job");
    }

}
