package com.ybt.seaotter.source.impl.file.ftp.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.source.impl.file.ftp.FtpConnector;
import com.ybt.seaotter.source.meta.file.FileMeta;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class FtpFileMeta implements FileMeta {

    private final FTPClient ftpClient;
    private final String filePath;
    private String separator;

    public FtpFileMeta(FtpConnector connector, FTPClient ftpClient, String filePath) {
        this.ftpClient = ftpClient;
        this.filePath = filePath;
        this.separator = connector.getSeparator();
    }

    @Override
    public List<String> columns() {

        List<String> columns = Lists.newArrayList();
        try {
            InputStream inputStream = ftpClient.retrieveFileStream(filePath);
            if (inputStream != null) {
                // 使用 BufferedReader 按行读取文件内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = reader.readLine();
                columns = Arrays.asList(StringUtils.splitByWholeSeparator(line, separator));
                // 关闭流
                reader.close();
                inputStream.close();
                // 完成文件读取
                ftpClient.completePendingCommand(); // 确保FTP命令完全执行
            } else {
                System.out.println("Failed to retrieve file.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    @Override
    public List<List<String>> rows(Integer limit) {
        List<List<String>> rows = Lists.newArrayList();
        try {
            InputStream inputStream = ftpClient.retrieveFileStream(filePath);
            if (inputStream != null) {
                // 使用 BufferedReader 按行读取文件内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                int index = 0;
                while ((line = reader.readLine()) != null) {
                    if (index > 0) {
                        rows.add(Arrays.asList(StringUtils.splitByWholeSeparator(line, separator)));
                    }
                    if (index >= limit) {
                        break;
                    }
                    index++;
                }
                // 关闭流
                reader.close();
                inputStream.close();
                // 完成文件读取
                ftpClient.completePendingCommand(); // 确保FTP命令完全执行
            } else {
                System.out.println("Failed to retrieve file.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }

    @Override
    public FileMeta separator(String separator) {
        this.separator = separator;
        return this;
    }

}
