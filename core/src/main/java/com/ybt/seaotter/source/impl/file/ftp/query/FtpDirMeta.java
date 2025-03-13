package com.ybt.seaotter.source.impl.file.ftp.query;

import com.google.common.collect.Lists;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.exceptions.SeaOtterException;
import com.ybt.seaotter.source.impl.file.ftp.FtpConnector;
import com.ybt.seaotter.source.meta.file.DirMeta;
import com.ybt.seaotter.source.meta.file.FileMeta;
import com.ybt.seaotter.source.pojo.FileObject;
import com.ybt.seaotter.source.pojo.enums.FileType;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class FtpDirMeta implements DirMeta {

    private final FtpConnector connector;
    private final FTPClient ftpClient;
    private final String[] WHITE_FILE_TYPES = new String[] {"csv", "txt", "xlsx", "xls"};

    public FtpDirMeta(FtpConnector connector, SeaOtterConfig config) {
        this.connector = connector;
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(connector.getHost());
            ftpClient.login(connector.getUsername(), connector.getPassword());
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setControlEncoding("UTF-8");
        } catch (IOException e) {
            throw new SeaOtterException("ftp连接失败");
        }
        this.ftpClient = ftpClient;
    }


    @Override
    public List<FileObject> list(String dir, List<String> formats) {
        try {
            dir = ftpClient.printWorkingDirectory().concat(dir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FTPFile[] files;
        try {
            files = ftpClient.listFiles(dir);
        } catch (IOException e) {
            throw new SeaOtterException(e);
        }
        List<FileObject> fileObjects = Lists.newArrayList();
        for (FTPFile file : files) {
            FileObject fileObject = new FileObject();
            fileObject.setName(file.getName());
            if (file.isFile()) {
                String suffix = file.getName().substring(file.getName().lastIndexOf(".") + 1);
                formats = formats.stream().map(String::toLowerCase).collect(Collectors.toList());
                if (!formats.contains(suffix.toLowerCase())) {
                    continue;
                }
                fileObject.setType(FileType.FILE);
            } else if (file.isDirectory()) {
                fileObject.setType(FileType.DIR);
            }
            fileObjects.add(fileObject);
        }
        return fileObjects;
    }

    @Override
    public FileMeta path(String fileName) {
        try {
            fileName = ftpClient.printWorkingDirectory().concat(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new FtpFileMeta(connector, ftpClient, fileName);
    }


}
