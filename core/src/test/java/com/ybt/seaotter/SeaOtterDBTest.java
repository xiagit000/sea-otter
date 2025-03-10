package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.enums.TransmissionMode;
import com.ybt.seaotter.config.FlinkOptions;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.config.SparkOptions;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.impl.db.dm.DmConnector;
import com.ybt.seaotter.source.impl.db.mysql.MysqlConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class SeaOtterDBTest {

    private SeaOtter seaOtter;

//    private final SourceConnector source = new MysqlConnector()
//            .setHost("172.16.2.47")
//            .setPort(3306)
//            .setUsername("saas_dba")
//            .setPassword("@Saas$2023")
//            .setDatabase("product_recognition")
//            .setTable("sync_task");

    @Before
    public void init() {
        SeaOtterConfig seaOtterConfig = SeaOtterConfig.builder()
                .sparkOptions(new SparkOptions("172.16.5.170", 6066))
                .flinkOptions(new FlinkOptions("172.16.5.170", 8081))
                .callback("http://192.168.10.5:9090/api/callback")
                .build();
        seaOtter = SeaOtter.config(seaOtterConfig);
    }

    private SourceConnector source = new DmConnector()
            .setHost("172.16.5.101")
            .setPort(5236)
            .setUsername("SYSDBA")
            .setPassword("Dameng111")
            .setDatabase("center_chain")
            .setTable("CHAIN_RECOVERY_ORDER");

//    private SourceConnector source = new OracleConnector()
//            .setHost("172.16.5.101")
//            .setPort(1521)
//            .setUsername("system")
//            .setPassword("oracle")
//            .setSid("xe")
//            .setDatabase("YBT")
//            .setTable("ORACLE_USER");
    private final SourceConnector sink = new StarrocksConnector()
            .setHost("172.16.1.51")
            .setHttpPort(8080)
            .setRpcPort(9030)
            .setUsername("root")
            .setPassword("")
            .setDatabase("data_warehouse")
            .setTable("CHAIN_RECOVERY_ORDER");

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
                .database("YBT").tables();
        System.out.println(JSON.toJSONString(databases));
    }

    /**
     * 查询table下的columns
     */
    @Test
    public void queryColumns() {
        List<String> databases = seaOtter.db(source)
                .database("YBT")
                .table("oracle_user")
                .columns();
        System.out.println(JSON.toJSONString(databases));
    }

    /**
     * 数据预览
     */
    @Test
    public void preview() {
        List<List<String>> databases = seaOtter.db(source)
                .database("YBT")
                .table("oracle_user")
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
                .tag("CDC123456") // 业务关联标签
                .from(source).to(sink).CDCMode().submit());
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

    /**
     * 取消实时任务
     * @param seaOtter
     */
    public void cdcJobCancel(SeaOtter seaOtter) {
        Boolean bool = seaOtter.job().CDCMode().cancel("857e866db3a9022a2122d88e7ecac1a3");
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
