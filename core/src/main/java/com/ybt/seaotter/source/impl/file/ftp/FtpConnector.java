package com.ybt.seaotter.source.impl.file.ftp;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.builder.FtpConnectorBuilder;
import com.ybt.seaotter.source.connector.FileSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.impl.file.ftp.migration.FtpDefine;
import com.ybt.seaotter.source.impl.file.ftp.query.FtpDirMeta;
import com.ybt.seaotter.source.meta.file.DirMeta;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FtpConnector implements FileSourceConnector {
    private String host;
    private Integer port;
    private String path;
    private String username;
    private String password;
    private String protocol;
    private String separator;

    public FtpConnector() {
    }

    public FtpConnector(FtpConnectorBuilder builder) {
        this.host = builder.getHost();
        this.port = builder.getPort();
        this.path = builder.getPath();
        this.username = builder.getUsername();
        this.password = builder.getPassword();
        this.separator = builder.getSeparator();
    }

    @Override
    public String getName() {
        return DataSourceType.FTP.name();
    }

    @Override
    public String getFlinkArgs() {
        return Arrays.stream(getSparkArgs()).collect(Collectors.joining(" "));
    }

    @Override
    public String[] getSparkArgs() {
        return new String[]{
                String.format("--ftp.host %s", host),
                String.format("--ftp.port %s", port),
                String.format("--ftp.username %s", username),
                String.format("--ftp.password %s", password),
                String.format("--ftp.path %s", path),
                String.format("--ftp.protocol %s", protocol)
        };
    }

    @Override
    public DirMeta getMeta(SeaOtterConfig config) {
        return new FtpDirMeta(this, config);
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new FtpDefine(this, sink);
    }

    public String getHost() {
        return host;
    }

    public FtpConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public FtpConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FtpConnector setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public FtpConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public FtpConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getProtocol() {
        return protocol;
    }

    public FtpConnector setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public String getSeparator() {
        return separator;
    }

    public FtpConnector setSeparator(String separator) {
        this.separator = separator;
        return this;
    }
}
