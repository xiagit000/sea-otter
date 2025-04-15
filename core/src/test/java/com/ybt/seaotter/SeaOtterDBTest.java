package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.enums.TransmissionMode;
import com.ybt.seaotter.config.FlinkOptions;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.config.SparkOptions;
import com.ybt.seaotter.source.connector.DBSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.impl.db.dm.DmConnector;
import com.ybt.seaotter.source.impl.db.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import me.tongfei.progressbar.ProgressBar;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class SeaOtterDBTest {

    private SeaOtter seaOtter;

    private final SourceConnector source = new MysqlConnector()
            .setHost("172.16.2.47")
            .setPort(3306)
            .setUsername("saas_dba")
            .setPassword("@Saas$2023")
            .setDatabase("search_boot")
            .setTable("sys_analyze_dict");

    @Before
    public void init() {
        SeaOtterConfig seaOtterConfig = SeaOtterConfig.builder()
                .sparkOptions(new SparkOptions("172.16.5.170", 6066))
                .flinkOptions(new FlinkOptions("127.0.0.1", 8081))
                .callback(null)
                .build();
        seaOtter = SeaOtter.config(seaOtterConfig);
    }

//    private SourceConnector source = new DmConnector()
//            .setHost("172.16.5.101")
//            .setPort(5236)
//            .setUsername("SYSDBA")
//            .setPassword("Dameng111")
//            .setDatabase("center_chain")
//            .setTable("CHAIN_SUPPLIER");

//    private SourceConnector source = new OracleConnector()
//            .setHost("172.16.5.101")
//            .setPort(1521)
//            .setUsername("system")
//            .setPassword("oracle")
//            .setSid("xe")
//            .setDatabase("YBT")
//            .setTable("ORACLE_USER");
//    private final DBSourceConnector source = new StarrocksConnector()
//            .setHost("172.16.1.51")
//            .setHttpPort(8080)
//            .setRpcPort(9030)
//            .setUsername("root")
//            .setPassword("");

//    private final DBSourceConnector source = new MysqlConnector()
//            .setHost("172.16.1.51")
//            .setPort(9030)
//            .setUsername("root")
//            .setPassword("");

    private final DBSourceConnector sink = new StarrocksConnector()
            .setHost("172.16.5.172")
            .setHttpPort(8040)
            .setRpcPort(9030)
            .setUsername("mar_service_all")
            .setPassword("Xznn2w19sc2")
            .setDatabase("data_warehouse")
            .setTable("sys_analyze_dict");

    /**
     * 查询database
     */
    @Test
    public void queryDatabase() {
        List<String> databases = seaOtter.db(source)
                .databases();
        System.out.println(JSON.toJSONString(databases));
    }

    /**
     * 查询database下的table
     */
    @Test
    public void queryTable() {
        List<String> databases = seaOtter.db(source)
                .database("data_warehouse").tables();
        System.out.println(JSON.toJSONString(databases));
    }

    /**
     * 查询table下的columns
     */
    @Test
    public void queryColumns() {
        List<String> databases = seaOtter.db(source)
                .database("data_warehouse")
                .table("bank_user")
                .columns();
        System.out.println(JSON.toJSONString(databases));
    }

    /**
     * 数据预览
     */
    @Test
    public void preview() {
        List<List<String>> databases = seaOtter.db(source)
                .database("data_warehouse")
                .table("bank_user")
                .rows(100);
        System.out.println(JSON.toJSONString(databases));
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() {
        seaOtter.job().from(source).to(sink).createTable();
    }

    @Test
    public void migrateDB() {
        String[] includeDatabases = {"mock_bank"};
        String[] includeTables = {"customer_group"};
        try (ProgressBar pb = new ProgressBar("processing", 1)) {
            for (String database : includeDatabases) {
//                List<String> tables = seaOtter.db(source).database(database).tables();
                for (String table : includeTables) {
//                    source.setDatabase(database);
//                    source.setTable(table);
                    sink.setDatabase(database);
                    sink.setTable(table);
                    SeaOtterJob seaOtterJob = seaOtter.job().from(source).to(sink);
//                    seaOtter.db(sink).database(database).table(table).drop();
                    seaOtterJob.createTable();
//                    String submitId = seaOtterJob.batchMode(TransmissionMode.UPSERT)
//                            .filter("create_time", "2025-04-07 00:21:01").submit();
//                    while (true) {
//                        try {
//                            Thread.sleep(3000);
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        }
//                        JobState detail = seaOtter.job().batchMode().detail(submitId);
//                        if (detail != JobState.CREATED && detail!= JobState.RUNNING) {
//                            System.out.printf("database:%s, table:%s, job state:%s%n", database, table, detail.toString());
//                            break;
//                        }
//                    }
                    pb.step();
                }
            }
        }
    }

    /**
     * 上传jar包
     */
    @Test
    public void uploadJar() {
        seaOtter.job().from(source).to(sink).CDCMode().uploadJar(new File("E:\\ybt\\sea-otter\\flink-job\\target\\flink-job-1.0-SNAPSHOT.jar"));
    }

    /**
     * 列出可执行jar包
     */
    @Test
    public void listJars() {
        System.out.println(seaOtter.job().from(source).to(sink).CDCMode().listJars());
    }

    /**
     * 实时同步任务
     */
    @Test
    public void cdcJob() {
        System.out.println(seaOtter.job()
                    .jobName("mysql to starrocks 3")
                    .tag("CDC123456") // 业务关联标签
                    .from(source).to(sink)
                    .CDCMode()
                        .serverId("5401-5405")
                    .submit());
    }

    /**
     * 批处理同步任务-全量覆盖
     */
    @Test
    public void batchJob() {
        System.out.println(seaOtter.job()
                .tag("BATCH123456") // 业务关联标签
                .from(source).to(sink)
                .batchMode(TransmissionMode.OVERWRITE)
                .submit());
    }

    /**
     * 批处理同步任务-增量
     * @param seaOtter
     * @return
     */
    public String batchJob2(SeaOtter seaOtter) {
        return seaOtter.job()
                .from(source).to(sink)
                .batchMode(TransmissionMode.UPSERT)
                .filter("start_time", "2023-12-01 17:21:01")
                .submit();
    }

    /**
     * 批处理查询job详情
     * @param seaOtter
     */
    public void batchJobDetail(SeaOtter seaOtter) {
        JobState detail = seaOtter.job().batchMode().detail("driver-20250228030439-0010");
        System.out.println(JSON.toJSONString(detail));
    }

    /**
     * 实时任务查询job详情
     * @param seaOtter
     */
    public void cdcJobDetail(SeaOtter seaOtter) {
        JobState jobState = seaOtter.job().CDCMode().detail("5effed2ab31f3e80940a5afde6082ce6");
        System.out.println(jobState.toString());
    }

    @Test
    public void cdcJobCancel() {
        Boolean bool = seaOtter.job().CDCMode().cancel("221cdd07204a214bfc650e46a1927d5d");
        System.out.println(bool);
    }

    /**
     * 取消批处理任务
     * @param seaOtter
     */
    public void batchJobCancel(SeaOtter seaOtter) {
        Boolean detail = seaOtter.job().batchMode().cancel("app-20250226054803-0013");
        System.out.println(detail);
    }

    public static void main(String[] args) {
        SeaOtterConfig seaOtterConfig = SeaOtterConfig.builder()
                .sparkOptions(new SparkOptions("localhost", 6066))
                .flinkOptions(new FlinkOptions("localhost", 8081))
                .callback("http://192.168.10.5:9090/api/callback")
                .build();
        SeaOtter seaOtter = SeaOtter.config(seaOtterConfig);

        SeaOtterDBTest seaOtterTest = new SeaOtterDBTest();
//        seaOtterTest.queryDatabase(seaOtter);
//        seaOtterTest.queryTable(seaOtter);
//        seaOtterTest.queryColumns(seaOtter);
//        seaOtterTest.preview(seaOtter);
//        seaOtterTest.cdcJob(seaOtter);
//          seaOtterTest.batchJob(seaOtter);
//        seaOtterTest.batchJobDetail(seaOtter);
//        seaOtterTest.batchJobCancel(seaOtter);
//        seaOtterTest.cdcJobDetail(seaOtter);
//        seaOtterTest.cdcJobCancel(seaOtter);
//        seaOtterTest.createTable(seaOtter);
    }
}
