package com.ybt.seaotter.source.impl.file.sftp.query;

import com.google.common.collect.Lists;
import com.jcraft.jsch.*;
import com.ybt.seaotter.config.SeaOtterConfig;
import com.ybt.seaotter.source.impl.file.sftp.SftpConnector;
import com.ybt.seaotter.source.meta.file.DirMeta;
import com.ybt.seaotter.source.meta.file.FileMeta;
import com.ybt.seaotter.source.pojo.FileObject;
import com.ybt.seaotter.source.pojo.enums.FileType;
import org.apache.commons.net.ftp.FTPFile;

import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class SftpDirMeta implements DirMeta {

    private final SftpConnector connector;

    public SftpDirMeta(SftpConnector connector, SeaOtterConfig config) {
        this.connector = connector;
    }


    @Override
    public List<FileObject> list(String dir, List<String> formats) {
        List<FileObject> fileObjects = Lists.newArrayList();
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
            Vector<ChannelSftp.LsEntry> sftpFiles = channel.ls(dir);
            for (ChannelSftp.LsEntry file : sftpFiles) {
                FileObject fileObject = new FileObject();
                fileObject.setName(file.getFilename());
                if (file.getAttrs().isDir()) {
                    fileObject.setType(FileType.DIR);
                } else {
                    String suffix = file.getFilename().substring(file.getFilename().lastIndexOf(".") + 1);
                    formats = formats.stream().map(String::toLowerCase).collect(Collectors.toList());
                    if (!formats.contains(suffix.toLowerCase())) {
                        continue;
                    }
                    fileObject.setType(FileType.FILE);
                }
                fileObjects.add(fileObject);
            }
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                channel.exit();
            }
            if (session != null) {
                session.disconnect();
            }
        }
        return fileObjects.stream()
                .filter(fileObject -> !fileObject.getName().equals(".") && !fileObject.getName().equals(".."))
                .collect(Collectors.toList());
    }

    @Override
    public FileMeta path(String fileName) {
        return new SftpFileMeta(connector, fileName);
    }


}
