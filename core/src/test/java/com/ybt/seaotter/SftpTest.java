package com.ybt.seaotter;

import com.jcraft.jsch.*;

import java.io.IOException;
import java.util.Vector;

public class SftpTest {

    public static void main(String[] args) throws IOException, JSchException, SftpException {
        JSch jsch = new JSch();
        Session session = jsch.getSession("martechdata", "172.16.5.170", 12222);
        session.setPassword("Ycb@martech789");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        Channel channel = session.openChannel("sftp");
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        sftpChannel.connect();
        Vector<ChannelSftp.LsEntry> files = sftpChannel.ls("/upload/spark/jars");
        for (ChannelSftp.LsEntry entry : files) {
            System.out.println(entry.getFilename());
        }
        sftpChannel.exit();
        session.disconnect();
    }
}
