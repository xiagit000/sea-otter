package com.ybt.seaotter.utils;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DownloadUtils {
    public static boolean downloadFile(String host, int port, String username, String password,
                                       String remoteFilePath, String localFilePath) {
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(host, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            File localFile = new File(localFilePath);
            File localDir = localFile.getParentFile();
            if (!localDir.exists() && !localDir.mkdirs()) {
                System.out.println("无法创建本地目录: " + localDir.getAbsolutePath());
                return false;
            }

            try (OutputStream outputStream = new FileOutputStream(localFilePath)) {
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
}
