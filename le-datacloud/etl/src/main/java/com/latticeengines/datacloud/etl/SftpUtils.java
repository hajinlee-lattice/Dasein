package com.latticeengines.datacloud.etl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;

public class SftpUtils {

    private static Log log = LogFactory.getLog(SftpUtils.class);

    public interface SftpFilenameFilter {
        boolean accept(String filename);
    }

    @SuppressWarnings("unchecked")
    public static final List<String> getFileNames(SftpConfiguration config,
            final String fileNamePattern) {
        try {
            log.info(String.format("Connecting to SFTP %s... ", config.getSftpHost()));
            Pattern pattern = fileNamePattern != null ? Pattern.compile(fileNamePattern) : null;
            JSch jsch = new JSch();
            Session session = jsch.getSession(config.getSftpUserName(), config.getSftpHost(),
                    config.getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(config.getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + config.getSftpDir());
            Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(".");
            List<String> fileSources = new ArrayList<String>();
            for (int i = 0; i < files.size(); i++) {
                ChannelSftp.LsEntry file = files.get(i);
                Matcher matcher = pattern != null ? pattern.matcher(file.getFilename()) : null;
                if (!file.getAttrs().isDir() && matcher != null && matcher.find()) {
                    fileSources.add(file.getFilename());
                }
            }
            sftpChannel.exit();
            session.disconnect();
            log.info(String.format("Disconnected with SFTP %s... ", config.getSftpHost()));
            return fileSources;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }

    public static final boolean ifFileExists(SftpConfiguration config, String fileName) {
        boolean res = false;
        try {
            log.info("Connecting to SFTP...");
            JSch jsch = new JSch();
            Session session = jsch.getSession(config.getSftpUserName(), config.getSftpHost(),
                    config.getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(config.getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + config.getSftpDir());
            InputStream is = sftpChannel.get(fileName);
            res = is != null;
            sftpChannel.exit();
            session.disconnect();
        } catch (JSchException | SftpException e) {
            res = false;
        }
        return res;
    }
}
