package com.ybt.seaotter.source.builder;

import com.ybt.seaotter.source.connector.FileSourceConnector;
import com.ybt.seaotter.source.impl.file.ftp.FtpConnector;
import com.ybt.seaotter.source.impl.file.sftp.SftpConnector;

public class FtpConnectorBuilder {
    private String host;
    private Integer port;
    private String path;
    private String username;
    private String password;
    private String protocol;
    private String separator;

    public static FtpConnectorBuilder builder() {
        return new FtpConnectorBuilder();
    }

    public FileSourceConnector build() {
        switch (protocol.toLowerCase()) {
            case "ftp":
                return new FtpConnector(this);
            case "sftp":
                return new SftpConnector(this);
            default:
                throw new RuntimeException("Unsupported protocol: " + protocol);
        }
    }

    public String getHost() {
        return host;
    }

    public FtpConnectorBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public FtpConnectorBuilder setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FtpConnectorBuilder setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public FtpConnectorBuilder setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public FtpConnectorBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getProtocol() {
        return protocol;
    }

    public FtpConnectorBuilder setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public String getSeparator() {
        return separator;
    }

    public FtpConnectorBuilder setSeparator(String separator) {
        this.separator = separator;
        return this;
    }
}
