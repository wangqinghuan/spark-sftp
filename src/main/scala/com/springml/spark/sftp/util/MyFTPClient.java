package com.springml.spark.sftp.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.springml.sftp.client.FileNameFilter;
import com.springml.sftp.client.ProgressMonitor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.util.Collection;
import java.util.Vector;
import java.util.logging.Logger;

import static org.apache.ivy.util.StringUtils.encrypt;

public class MyFTPClient {
    private static final Logger LOG = Logger.getLogger(MyFTPClient.class.getName());


    private String username;
    private String password;
    private String host;
    private int port;


    public MyFTPClient(String username, String password,String host, int port) {

        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;

    }
    private FTPClient connectFtp() throws Exception {
        FTPClient ftp = new FTPClient();

        ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));

        ftp.connect(host,port);
        int reply = ftp.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftp.disconnect();
            throw new IOException("Exception in connecting to FTP Server");
        }

        ftp.login(username,password);
        return ftp;
    }
    public String copy(String source, String target) throws Exception {
        FTPClient ftpClient = connectFtp();
        copyInternal(ftpClient, source, target);
        releaseConnection(ftpClient);
        LOG.info("Copied files successfully...");

        return target;
    }

    private void releaseConnection(org.apache.commons.net.ftp.FTPClient ftpClient) throws IOException {
        ftpClient.disconnect();
    }

    public String copyLatest(String source, String target) throws Exception {
        FTPClient ftpClient = connectFtp();
        String latestSource = getLatestSource(ftpClient, source);
        copyInternal(ftpClient, latestSource, target);
        releaseConnection(ftpClient);
        LOG.info("Copied files successfully...");

        return getCopiedFilePath(latestSource, target);
    }

    public String copyLatestToFTP(String source, String target) throws Exception {
        FTPClient ftpClient = connectFtp();
        String latestSource = getLatestLocalSource(source);
        copyInternalToFTP(ftpClient, latestSource, target);
        releaseConnection(ftpClient);
        LOG.info("Copied files successfully...");

        return getCopiedFilePath(latestSource, target);
    }

    public String copyToFTP(String source, String target) throws Exception {
        FTPClient ftpClient = connectFtp();
        copyInternalToFTP(ftpClient, source, target);
        releaseConnection(ftpClient);
        LOG.info("Copied files successfully...");

        return target;
    }

    private String getCopiedFilePath(String latestSource, String target) {
        String copiedFileName = FilenameUtils.getName(latestSource);
        return FilenameUtils.concat(target, copiedFileName);
    }

    private String getLatestSource(FTPClient ftpClient, String source) throws Exception {
        FTPFile[] ls = ftpClient.listFiles();

        String basePath = FilenameUtils.getPath(source);
        if (!basePath.startsWith("/")) {
            basePath = "/" + basePath;
        }

        LOG.fine("Base Path : " + basePath);
        long latestModTime = 0;
        String fileName = FilenameUtils.getBaseName(source);
        for (int i = 0, size = ls.length; i < size; i++) {
            FTPFile entry = ls[i];
            long modTime = entry.getTimestamp().getTimeInMillis();
            if (latestModTime < modTime) {
                latestModTime = modTime;
                fileName = entry.getName();
            }
        }

        return FilenameUtils.concat(basePath, fileName);
    }

    private String getLatestLocalSource(String source) throws Exception {
        String fileName = FilenameUtils.getBaseName(source);
        String basePath = FilenameUtils.getPath(source);
        if (!basePath.startsWith("/")) {
            basePath = "/" + basePath;
        }

        File baseDir = new File(basePath);
        File[] filteredFiles = baseDir.listFiles(new FileNameFilter(fileName));

        LOG.fine("Base Path : " + basePath);
        long latestModTime = 0;
        for (int i = 0; i < filteredFiles.length; i++) {
            long modTime = filteredFiles[i].lastModified();
            if (latestModTime < modTime) {
                latestModTime = modTime;
                fileName = filteredFiles[i].getName();
            }
        }

        return FilenameUtils.concat(basePath, fileName);
    }

    private void copyInternal(FTPClient ftpClient, String source, String target) throws Exception {
        LOG.info("Copying file from " + source + " to " + target);
        try {
            ftpClient.changeWorkingDirectory(source);
            copyDir(ftpClient, source, target);
        } catch (Exception e) {
            // Source is a file
            OutputStream outputStream =
                    new BufferedOutputStream(new FileOutputStream(target + source));
            boolean success = ftpClient.retrieveFile(source, outputStream);
        }
    }

    private void copyDir(FTPClient ftpClient, String source, String target) throws Exception {
        LOG.info("Copying files from " + source + " to " + target);

        ftpClient.changeWorkingDirectory(source);
         FTPFile[] childFiles = ftpClient.listFiles();
        for ( FTPFile lsEntry : childFiles) {
            String entryName = lsEntry.getName();
            LOG.fine("File Entry " + entryName);

            if (!entryName.equals(".") && !entryName.equals("..")) {
                if (lsEntry.isDirectory()) {
                    copyInternal(ftpClient, source + entryName + "/", target);
                } else {
                    LOG.info("Copying file " + entryName);
                    OutputStream outputStream =
                            new BufferedOutputStream(new FileOutputStream(target + entryName));
                    ftpClient.retrieveFile(entryName, outputStream);
                }
            }
        }
    }



    private void copyInternalToFTP(FTPClient ftpClient, String source, String target) throws Exception {
        LOG.info("Copying files from " + source + " to " + target);

            File file = new File(source);
            if(file.isDirectory()){
                copyDirToFTP(ftpClient, source, target);
            }else{
                LOG.info("Copying file " + source);
                ftpClient.storeFile(target+file.getName(),new FileInputStream(file));
            }


    }

    private void copyDirToFTP(FTPClient ftpClient, String source, String target) throws Exception {
        LOG.info("Copying files from " + source + " to " + target);

        ftpClient.changeWorkingDirectory(target);

        Collection<File> childFiles = FileUtils.listFiles(new File(source), null, false);
        for (File file : childFiles) {
            String entryName = file.getName();
            LOG.fine("File Entry " + entryName);

            if (!entryName.equals(".") && !entryName.equals("..")) {
                if (file.isDirectory()) {
                    copyInternalToFTP(ftpClient, source + entryName + "/", target);
                } else {
                    LOG.info("Copying file " + entryName);
                    ftpClient.storeFile(target+file.getName(),new FileInputStream(file));
                }
            }
        }
    }


}
