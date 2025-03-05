package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;

import java.io.File;

public class FlinkSubmitter {

    public void submit() {
    }

    public static void main(String[] args) throws ApiException {
        FlinkApi api = new FlinkApi();
        api.getApiClient().setBasePath("http://localhost:8081");
//        JarListInfo jars = api.listJars();
//        System.out.println(JSON.toJSONString(jars));
        JarUploadResponseBody result = api.uploadJar(new File("E:\\ybt\\sea-otter\\flink-job\\target\\flink-job-1.0-SNAPSHOT.jar"));
        System.out.println(JSON.toJSONString(result.getFilename()));
//        JarRunResponseBody response = api.runJar("7df18d8f-969f-49a9-baa1-f0705b87a258_flink-job-1.0-SNAPSHOT.jar",
//                true, null, "--INPUT A --OUTPUT B", null,
//                "com.ybt.seaotter.MysqlCDCPipeline", null);
//        System.out.println(JSON.toJSONString(response));
    }
}
