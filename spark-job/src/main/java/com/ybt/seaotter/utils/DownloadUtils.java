package com.ybt.seaotter.utils;

import com.jcraft.jsch.*;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DownloadUtils {
    
    public static boolean downloadByFtp(String host, int port, String username, String password,
                                        String remoteFilePath, String localFilePath) {

        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(host, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            File localFile = new File(localFilePath);
            File localDir = localFile.getParentFile();
            if (!localDir.exists() && !localDir.mkdirs()) {
                System.out.println("无法创建本地目录: " + localDir.getAbsolutePath());
                return false;
            }
            try {
                remoteFilePath = ftpClient.printWorkingDirectory().concat(remoteFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try (OutputStream outputStream = Files.newOutputStream(Paths.get(localFilePath))) {
                return ftpClient.retrieveFile(remoteFilePath, outputStream);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        } finally {
            try {
                ftpClient.logout();
                ftpClient.disconnect();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void downloadBySftp(String host, int port, String username, String password,
                                        String remoteFilePath, String localFilePath) {
        Session session = null;
        ChannelSftp sftpChannel = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            Channel channel = session.openChannel("sftp");
            sftpChannel = (ChannelSftp) channel;
            sftpChannel.connect();

            File localFile = new File(localFilePath);
            File localDir = localFile.getParentFile();
            if (!localDir.exists() && !localDir.mkdirs()) {
                System.out.println("无法创建本地目录: " + localDir.getAbsolutePath());
            }

            try (OutputStream outputStream = Files.newOutputStream(Paths.get(localFilePath))) {
                sftpChannel.get(remoteFilePath, outputStream);
            } catch (SftpException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException | JSchException ex) {
            ex.printStackTrace();
        } finally {
            if (sftpChannel != null) {
                sftpChannel.exit();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }
}
