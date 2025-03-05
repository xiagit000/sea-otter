package com.ybt.seaotter.common.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;

public class HttpExecutor {
    private final static String CHARSET_ENCODING = "utf-8";

    public static String get(String api_url, String charset, String authorization) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = HttpExecutor.toUri(api_url);
            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader("Authorization", authorization);
            httpGet.setHeader("Accept", "application/json");
            httpGet.setHeader("Content-Type", "application/json");
            httpGet.setHeader("Accept-Charset", "utf-8");
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, charset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String get(String api_url, Map<String, String> headers, Integer timeout) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = HttpExecutor.toUri(api_url);
            HttpGet httpGet = new HttpGet(uri);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeout) // 设置连接超时时间
                    .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                    .setSocketTimeout(timeout) // 设置数据读取超时时间
                    .build();
            httpGet.setConfig(requestConfig);
            headers.forEach(httpGet::setHeader);
            httpGet.setHeader("Accept", "application/json");
            httpGet.setHeader("Content-Type", "application/json");
            httpGet.setHeader("Accept-Charset", "utf-8");
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String get(String api_url) {
        return get(api_url, CHARSET_ENCODING);
    }

    public static String get(String api_url, Integer timeout) {
        return get(api_url, CHARSET_ENCODING, timeout);
    }

    public static String get(String api_url, String charset) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpGet httpGet = new HttpGet(uri);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, charset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String get(String api_url, String charset, Integer timeout) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpGet httpGet = new HttpGet(uri);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeout) // 设置连接超时时间
                    .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                    .setSocketTimeout(timeout) // 设置数据读取超时时间
                    .build();
            httpGet.setConfig(requestConfig);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, charset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String sslPost(String keyStorePath, String pass, String url, Object data) {
        KeyStore keyStore = null;
        FileInputStream instream = null;
        String result = null;
        try {
            keyStore = KeyStore.getInstance("PKCS12");
            instream = new FileInputStream(new File(keyStorePath));
            keyStore.load(instream, pass.toCharArray());
        } catch (CertificateException | NoSuchAlgorithmException | IOException | KeyStoreException e) {
            e.printStackTrace();
        } finally {
            try {
                if (instream != null) {
                    instream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // Trust own CA and all self-signed certs
        SSLContext sslcontext = null;
        try {
            sslcontext = SSLContexts.custom()
                    .loadKeyMaterial(keyStore, pass.toCharArray())
                    .build();
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException | UnrecoverableKeyException e) {
            e.printStackTrace();
        }
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext, new String[]{"TLSv1"}, null, SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
        CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
        try {
            URI uri = toUri(url);
            HttpPost httpPost = new HttpPost(uri);
            StringEntity stringEntity = new StringEntity(data.toString(), CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType("text/xml");
            httpPost.setEntity(stringEntity);
            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();
                result = EntityUtils.toString(entity, CHARSET_ENCODING);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpclient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static String post(String api_url, String json, String authorization) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setHeader("Authorization", authorization);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Accept-Charset", "utf-8");
            StringEntity stringEntity = new StringEntity(json, CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, String json, String authorization, Integer timeout) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeout) // 设置连接超时时间
                    .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                    .setSocketTimeout(timeout) // 设置数据读取超时时间
                    .build();
            httpPost.setConfig(requestConfig);
            httpPost.setHeader("Authorization", authorization);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Accept-Charset", "utf-8");
            StringEntity stringEntity = new StringEntity(json, CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, List<NameValuePair> params) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setEntity(new UrlEncodedFormEntity(params, CHARSET_ENCODING));
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, List<NameValuePair> params, Integer timeout) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            if (timeout != null) {
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout(timeout) // 设置连接超时时间
                        .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                        .setSocketTimeout(timeout) // 设置数据读取超时时间
                        .build();
                httpPost.setConfig(requestConfig);
            }
            httpPost.setEntity(new UrlEncodedFormEntity(params, CHARSET_ENCODING));
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, List<NameValuePair> params, Map<String, String> headers, Integer timeout) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            if (timeout != null) {
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout(timeout) // 设置连接超时时间
                        .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                        .setSocketTimeout(timeout) // 设置数据读取超时时间
                        .build();
                httpPost.setConfig(requestConfig);
            }
            headers.forEach(httpPost::setHeader);
            httpPost.setEntity(new UrlEncodedFormEntity(params, CHARSET_ENCODING));
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, Object json, Integer timeout) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeout) // 设置连接超时时间
                    .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                    .setSocketTimeout(timeout) // 设置数据读取超时时间
                    .build();
            httpPost.setConfig(requestConfig);
            StringEntity stringEntity = new StringEntity(json.toString(), CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, Object json) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(3000)  // 连接超时（毫秒）
                .setSocketTimeout(3000)   // 读取超时（毫秒）
                .build();
        HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            StringEntity stringEntity = new StringEntity(json.toString(), CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, Object json, String contentType) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            StringEntity stringEntity = new StringEntity(json.toString(), CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType(contentType);
            httpPost.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, Map<String, String> headers, String body) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            headers.forEach(httpPost::setHeader);
            StringEntity stringEntity = new StringEntity(body, CHARSET_ENCODING);
            stringEntity.setContentEncoding(CHARSET_ENCODING);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, Map<String, String> headers, List<NameValuePair> params) {
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        try {
            URI uri = toUri(api_url);
            HttpPost httpPost = new HttpPost(uri);
            headers.entrySet().stream().forEach(e -> httpPost.setHeader(e.getKey(), e.getValue()));
            httpPost.setEntity(new UrlEncodedFormEntity(params, CHARSET_ENCODING));
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String post(String api_url, Map<String, String> headers, String body, Integer timeout) throws IOException {//timeout单位毫秒（5000表示5秒）
        HttpClient httpClient = HttpClients.createDefault();
        String result = null;
        URI uri = toUri(api_url);
        HttpPost httpPost = new HttpPost(uri);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(timeout) // 设置连接超时时间
                .setConnectionRequestTimeout(timeout) // 设置请求超时时间
                .setSocketTimeout(timeout) // 设置数据读取超时时间
                .build();
        httpPost.setConfig(requestConfig);
        headers.forEach(httpPost::setHeader);
        StringEntity stringEntity = new StringEntity(body, CHARSET_ENCODING);
        stringEntity.setContentEncoding(CHARSET_ENCODING);
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        result = EntityUtils.toString(entity, CHARSET_ENCODING);
        return result;
    }

    public static String sslGet(String api_url) {
        String result = null;
        try {
            SSLConnectionSocketFactory scsf = new SSLConnectionSocketFactory(
                    SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(),
                    NoopHostnameVerifier.INSTANCE);
            HttpClient httpClient = HttpClients.custom().setSSLSocketFactory(scsf).build();
            URI uri = toUri(api_url);
            HttpGet httpGet = new HttpGet(uri);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, CHARSET_ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static URI toUri(String url) {
        URI uri = null;
        try {
            URL surl = new URL(url);
            uri = surl.toURI();
        } catch (MalformedURLException | URISyntaxException e) {
            e.printStackTrace();
        }
        return uri;
    }
}
