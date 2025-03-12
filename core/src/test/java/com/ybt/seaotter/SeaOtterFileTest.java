package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.ybt.seaotter.common.enums.JobState;
import com.ybt.seaotter.common.enums.TransmissionMode;
import com.ybt.seaotter.config.FlinkOptions;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.config.SparkOptions;
import com.ybt.seaotter.source.builder.FtpConnectorBuilder;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.impl.file.ftp.FtpConnector;
import com.ybt.seaotter.source.impl.db.starrocks.StarrocksConnector;
import com.ybt.seaotter.source.meta.file.FileMeta;
import com.ybt.seaotter.source.pojo.FileObject;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SeaOtterFileTest {

    private SeaOtter seaOtter;

//    private SourceConnector source = new MysqlConnector()
//            .setHost("172.16.2.47")
//            .setPort("3306")
//            .setUsername("saas_dba")
//            .setPassword("@Saas$2023")
//            .setDatabase("product_recognition")
//            .setTable("sync_task");

    @Before
    public void init() {
        SeaOtterConfig seaOtterConfig = SeaOtterConfig.builder()
                .sparkOptions(new SparkOptions("localhost", 6066))
                .flinkOptions(new FlinkOptions("localhost", 8081))
                .callback("http://192.168.10.5:9090/api/callback")
                .build();
        seaOtter = SeaOtter.config(seaOtterConfig);
    }

    private final SourceConnector source = FtpConnectorBuilder
            .builder()
            .setHost("172.16.5.170")
            .setPort(21)
            .setUsername("ftpuser")
            .setPassword("123")
            .setProtocol("ftp")
            .setSeparator(",")
            .setPath("/output1.csv")
            .build();
//    private final SourceConnector source = FtpConnectorBuilder
//            .builder()
//            .setHost("172.16.5.170")
//            .setPort(12222)
//            .setUsername("martechdata")
//            .setPassword("Ycb@martech789")
//            .setProtocol("sftp")
//            .setSeparator(",")
//            .setPath("/upload/output1.csv")
//            .build();
    private final SourceConnector sink = new StarrocksConnector()
            .setHost("172.16.1.51")
            .setHttpPort(8080)
            .setRpcPort(9030)
            .setUsername("root")
            .setPassword("")
            .setDatabase("data_warehouse")
            .setTable("product_sample_01");

    /**
     * 显示目录下的文件列表
     */
    @Test
    public void listFiles() {
        List<FileObject> files = seaOtter.file(source).list("/", Lists.newArrayList("txt"));
        System.out.println(JSON.toJSONString(files));
    }

    /**
     * 查询文件的数据列
     */
    @Test
    public void queryColumns() {
        List<String> columns = seaOtter.file(source)
                .path("/upload/output1.csv")
                .columns();
        System.out.println(JSON.toJSONString(columns));
    }

    /**
     * 数据预览
     */
    @Test
    public void preview() {
        FileMeta meta = seaOtter.file(source)
                .path("/upload/output1.csv");
        List<String> columns = meta.columns();
        List<List<String>> databases = meta
                .rows(2);
        System.out.println(JSON.toJSONString(columns));
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
     * 批处理查询job详情
     * @param seaOtter
     */
    public void batchJobDetail(SeaOtter seaOtter) {
        JobState detail = seaOtter.job().batchMode().detail("driver-20250228030439-0010");
        System.out.println(JSON.toJSONString(detail));
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

        SeaOtterFileTest seaOtterTest = new SeaOtterFileTest();
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
