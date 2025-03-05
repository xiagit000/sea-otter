package com.ybt.seaotter;

import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.SparkRestClient;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class SparkSubmitter {

    public void submit() {
        String sparkUrl = "http://localhost:6066/v1/submissions/create";

        // 提交任务的 JSON 请求体
        String json = "{\n" +
                "  \"appResource\": \"file:///opt/jars/spark-job-1.0-SNAPSHOT.jar\",\n" +
                "  \"sparkProperties\": {\n" +
                "    \"spark.app.name\": \"Spark Pi\",\n" +
                "    \"spark.driver.memory\": \"1g\",\n" +
                "    \"spark.driver.cores\": \"1\",\n" +
                "    \"spark.executor.memory\": \"1g\",\n" +
                "    \"spark.executor.instances\": \"1\",\n" +
                "    \"spark.executor.cores\": \"1\",\n" +
                "    \"spark.jars\": \"file:///opt/jars/mysql-connector-java-8.0.28.jar,file:///opt/jars/starrocks-spark-connector-3.4_2.12-1.1.2.jar\",\n" +
                "    \"spark.driver.extraClassPath\": \"file:///opt/jars/mysql-connector-java-8.0.28.jar\",\n" +
                "    \"spark.executor.extraClassPath\": \"file:///opt/jars/mysql-connector-java-8.0.28.jar\",\n" +
                "    \"spark.executor.extraJavaOptions\": \"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\",\n" +
                "    \"spark.driver.extraJavaOptions\": \"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\"\n" +
                "  },\n" +
                "  \"clientSparkVersion\": \"\",\n" +
                "  \"mainClass\": \"com.ybt.seaotter.MysqlPipeline\",\n" +
                "  \"environmentVariables\": { },\n" +
                "  \"action\": \"CreateSubmissionRequest\",\n" +
                "  \"appArgs\": []\n" +
                "}";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(sparkUrl);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(json));

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                String responseString = EntityUtils.toString(entity);
                System.out.println("Response: " + responseString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws FailedSparkRequestException {
        SparkRestClient sparkRestClient = SparkRestClient.builder()
                .masterHost("localhost")
                .sparkVersion("3.4.3")
                .build();
        final String submissionId = sparkRestClient.prepareJobSubmit()
                .appName("MySparkJob!")
                .appResource("file:///opt/jars/spark-job-1.0-SNAPSHOT.jar")
                .mainClass("com.ybt.seaotter.MysqlPipeline")
                .withProperties()
                .put("spark.driver.memory", "1g")
                .put("spark.driver.cores", "1")
                .put("spark.executor.memory", "1g")
                .put("spark.executor.instances", "1")
                .put("spark.executor.cores", "1")
                .put("spark.jars", "file:///opt/jars/mysql-connector-java-8.0.28.jar,file:///opt/jars/starrocks-spark-connector-3.4_2.12-1.1.2.jar")
                .put("spark.driver.extraClassPath", "file:///opt/jars/mysql-connector-java-8.0.28.jar")
                .put("spark.executor.extraClassPath", "file:///opt/jars/mysql-connector-java-8.0.28.jar")
                .put("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .put("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .submit();
        System.out.println(submissionId);
//        new SparkSubmitter().submit();
    }
}
