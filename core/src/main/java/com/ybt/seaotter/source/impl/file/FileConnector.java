package com.ybt.seaotter.source.impl.file;

import com.ybt.seaotter.common.enums.DataSourceType;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.connector.FileSourceConnector;
import com.ybt.seaotter.source.connector.SourceConnector;
import com.ybt.seaotter.source.ddl.DataDefine;
import com.ybt.seaotter.source.impl.file.migration.FileDefine;
import com.ybt.seaotter.source.impl.file.query.FtpDirMeta;
import com.ybt.seaotter.source.impl.mysql.migration.MysqlDefine;
import com.ybt.seaotter.source.meta.file.DirMeta;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FileConnector implements FileSourceConnector {
    private String host;
    private Integer port;
    private String path;
    private String username;
    private String password;
    private String protocol;
    private String separator;

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
                String.format("--file.host %s", host),
                String.format("--file.port %s", port),
                String.format("--file.username %s", username),
                String.format("--file.password %s", password),
                String.format("--file.path %s", path),
                String.format("--file.protocol %s", protocol)
        };
    }

    @Override
    public DirMeta getMeta(SeaOtterConfig config) {
        return new FtpDirMeta(this, config);
    }

    @Override
    public DataDefine getDataDefine(SourceConnector sink) {
        return new FileDefine(this, sink);
    }

    public String getHost() {
        return host;
    }

    public FileConnector setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public FileConnector setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FileConnector setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public FileConnector setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public FileConnector setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getProtocol() {
        return protocol;
    }

    public FileConnector setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public String getSeparator() {
        return separator;
    }

    public FileConnector setSeparator(String separator) {
        this.separator = separator;
        return this;
    }
}
