package com.ybt.seaotter.source.impl.file.sftp.query;

import com.google.common.collect.Lists;
import com.jcraft.jsch.*;
import com.ybt.seaotter.source.impl.file.sftp.SftpConnector;
import com.ybt.seaotter.source.meta.file.FileMeta;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class SftpFileMeta implements FileMeta {

    private final String filePath;
    private final SftpConnector connector;
    private String separator;

    public SftpFileMeta(SftpConnector connector, String filePath) {
        this.filePath = filePath;
        this.connector = connector;
    }

    @Override
    public List<String> columns() {
        List<String> columns = Lists.newArrayList();
        Session session = null;
        ChannelSftp channel = null;
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(connector.getUsername(), connector.getHost(), connector.getPort());
            session.setPassword(connector.getPassword());
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            InputStream inputStream = channel.get(filePath);
            if (inputStream != null) {
                // 使用 BufferedReader 按行读取文件内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = reader.readLine();
                columns = Arrays.asList(line.split(separator == null ? "," : separator));
                // 关闭流
                reader.close();
                // 完成文件读取
                inputStream.close();
            } else {
                System.out.println("Failed to retrieve file.");
            }
        } catch (JSchException | SftpException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                channel.exit();
            }
            if (session != null) {
//                session.disconnect();
            }
        }
        return columns;
    }

    @Override
    public List<List<String>> rows(Integer limit) {
        List<List<String>> rows = Lists.newArrayList();
        Session session = null;
        ChannelSftp channel = null;
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(connector.getUsername(), connector.getHost(), connector.getPort());
            session.setPassword(connector.getPassword());
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            InputStream inputStream = channel.get(filePath);
            if (inputStream != null) {
                // 使用 BufferedReader 按行读取文件内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                int index = 0;
                while ((line = reader.readLine()) != null) {
                    if (index > 0) {
                        rows.add(Arrays.asList(line.split(separator == null ? "," : separator)));
                    }
                    if (index >= limit) {
                        break;
                    }
                    index++;
                }
                // 关闭流
                reader.close();
                inputStream.close();
            } else {
                System.out.println("Failed to retrieve file.");
            }
        } catch (JSchException | SftpException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                channel.exit();
            }
            if (session != null) {
//                session.disconnect();
            }
        }
        return rows;
    }

    @Override
    public FileMeta separator(String separator) {
        this.separator = separator;
        return this;
    }

}
