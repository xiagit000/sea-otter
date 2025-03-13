package com.ybt.seaotter.source.impl.file.sftp;

import com.google.common.base.Strings;
import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.builder.FtpConnectorBuilder;
import com.ybt.seaotter.source.connector.FileSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.impl.file.sftp.migration.SftpDefine;
import com.ybt.seaotter.source.impl.file.sftp.query.SftpDirMeta;
import com.ybt.seaotter.source.meta.file.DirMeta;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SftpConnector implements FileSourceConnector {
    private String host;
    private Integer port;
    private String path;
    private String username;
    private String password;
    private String separator = ",";

    public SftpConnector() {
    }

    public SftpConnector(FtpConnectorBuilder builder) {
        this.host = builder.getHost();
        this.port = builder.getPort();
        this.path = builder.getPath();
        this.username = builder.getUsername();
        this.password = builder.getPassword();
        if (!Strings.isNullOrEmpty(builder.getSeparator())) {
            this.separator = builder.getSeparator();
        }
    }

    @Override
    public String getName() {
        return DataSourceType.SFTP.name();
    }

    @Override
    public String getFlinkArgs() {
        return Arrays.stream(getSparkArgs()).collect(Collectors.joining(" "));
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--sftp.host %s", host),
                String.format("--sftp.port %s", port),
                String.format("--sftp.username %s", username),
                String.format("--sftp.password %s", password),
                String.format("--sftp.path %s", path),
                String.format("--sftp.separator %s", separator),
        };
    }

    @Override
    public DirMeta getMeta(SeaOtterConfig config) {
        return new SftpDirMeta(this, config);
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new SftpDefine(this, sink);
    }

    public String getHost() {
        return host;
    }

    public SftpConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public SftpConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getPath() {
        return path;
    }

    public SftpConnector setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public SftpConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SftpConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getSeparator() {
        return separator;
    }

    public SftpConnector setSeparator(String separator) {
        this.separator = separator;
        return this;
    }
}
