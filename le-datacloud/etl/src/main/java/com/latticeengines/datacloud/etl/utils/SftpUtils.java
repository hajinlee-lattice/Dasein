package com.latticeengines.datacloud.etl.utils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;

public class SftpUtils {

    private static Logger log = LoggerFactory.getLogger(SftpUtils.class);

    public interface SftpFilenameFilter {
        boolean accept(String filename);
    }

    /**
     * Check whether a file existed on SFTP
     *
     * TODO: Support sub-folder
     *
     * @param config
     * @param fileName
     * @return
     */
    public static final boolean ifFileExists(@NotNull SftpConfiguration config, @NotNull String fileName) {
        validateSftpConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(fileName));

        try {
            log.info("Connecting to SFTP...");
            JSch jsch = new JSch();
            Session session = jsch.getSession(config.getSftpUserName(), config.getSftpHost(), config.getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(config.getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + getSanityPath(config.getSftpDir(), true));

            InputStream is = sftpChannel.get(fileName);
            boolean existed = is != null;

            sftpChannel.exit();
            session.disconnect();

            return existed;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Get file list from SFTP which satisfies directory/file naming pattern and
     * version check strategy
     *
     * @param config
     * @param fileNamePattern
     * @return
     */
    public static final List<String> getFileNames(SftpConfiguration config, final String fileNamePattern) {
        validateSftpConfig(config);
        return getFileNames(config, fileNamePattern, null);
    }

    /**
     * For testing purpose, pass in Calendar instance instead of always using
     * current time base
     * 
     * TODO: Could get rid of parameter fileNamePattern and derive it from
     * SftpConfiguration
     *
     * @param config
     * @param fileNamePattern
     * @param calendar
     * @return
     */
    @VisibleForTesting
    static final List<String> getFileNames(@NotNull SftpConfiguration config, final String fileNamePattern,
            Calendar calendar) {
        try {
            log.info(String.format("Connecting to SFTP %s... ", config.getSftpHost()));

            JSch jsch = new JSch();
            Session session = jsch.getSession(config.getSftpUserName(), config.getSftpHost(),
                    config.getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(config.getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + getSanityPath(config.getSftpDir(), true));

            List<String> scanDirs = getScanDirs(config, sftpChannel, calendar);
            List<String> scanFiles = getScanFiles(config, sftpChannel, calendar, fileNamePattern, scanDirs);

            sftpChannel.exit();
            session.disconnect();
            log.info(String.format("Disconnected with SFTP %s... ", config.getSftpHost()));

            return scanFiles;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get directory list to scan. If there is sub-folder, return sub-folder
     * list whose name satisfies name pattern. No recursive searching.
     * 
     * @param config
     * @param channel
     * @param calendar
     * @return
     */
    @SuppressWarnings("unchecked")
    private static List<String> getScanDirs(@NotNull SftpConfiguration config, @NotNull ChannelSftp channel,
            Calendar calendar) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(channel);

        if (!config.hasSubFolder()) {
            return Arrays.asList("");
        }
        try {
            List<String> scanDirs = new ArrayList<>();
            Vector<ChannelSftp.LsEntry> paths = channel.ls(".");
            for (ChannelSftp.LsEntry path : paths) {
                if (path.getAttrs().isDir()) {
                    scanDirs.add(getSanityPath(path.getFilename(), true));
                }
            }
            return VersionUtils.getMostRecentVersionPaths(scanDirs, config.getCheckVersion(), config.getCheckStrategy(),
                    config.getSubFolderTSPattern(), calendar);
        } catch (SftpException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get file list under scanned directory which satisfies file name pattern.
     * If file is under sub-folder, append sub-folder before file name too. No
     * recursive searching.
     * 
     * TODO: Get rid of parameter fileNamePattern which could be derived from
     * SftpConfiguration
     *
     * @param config
     * @param channel
     * @param calendar
     * @param fileNamePattern
     * @param scanDirs
     * @return
     */
    @SuppressWarnings("unchecked")
    private static List<String> getScanFiles(@NotNull SftpConfiguration config, @NotNull ChannelSftp channel,
            Calendar calendar, String fileNamePattern, @NotNull List<String> scanDirs) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(channel);
        Preconditions.checkNotNull(scanDirs);
        Pattern pattern = fileNamePattern != null ? Pattern.compile(fileNamePattern) : null;

        List<String> scanFiles = new ArrayList<>();
        try {
            for (String dir : scanDirs) {
                dir = getSanityPath(dir, true);
                Vector<ChannelSftp.LsEntry> files = channel.ls("." + dir);
                for (int i = 0; i < files.size(); i++) {
                    ChannelSftp.LsEntry file = files.get(i);
                    if (file.getAttrs().isDir()) {
                        continue;
                    }
                    if (pattern == null) {
                        scanFiles.add(getSanityPath(appendPath(dir, file.getFilename()), false));
                        continue;
                    }
                    Matcher matcher = pattern.matcher(file.getFilename());
                    if (matcher.find()) {
                        scanFiles.add(getSanityPath(appendPath(dir, file.getFilename()), false));
                    }
                }
            }
        } catch (SftpException e) {
            throw new RuntimeException(e);
        }
        return VersionUtils.getMostRecentVersionPaths(scanFiles, config.getCheckVersion(), config.getCheckStrategy(),
                config.getFileTimestamp(), calendar);
    }

    private static void validateSftpConfig(SftpConfiguration config) {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(config.getSftpHost()));
        Preconditions.checkNotNull(config.getSftpPort());
        Preconditions.checkArgument(StringUtils.isNotBlank(config.getSftpUserName()));
        Preconditions.checkArgument(StringUtils.isNotBlank(config.getSftpPasswordEncrypted()));
        Preconditions.checkNotNull(config.getCheckStrategy());
    }

    private static String getSanityPath(String path, boolean withLeadingSlash) {
        if (StringUtils.isBlank(path)) {
            return "";
        }
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (withLeadingSlash) {
            return "/" + path;
        }
        return path;
    }

    private static String appendPath(String dir, String fileName) {
        if (StringUtils.isBlank(dir)) {
            return fileName;
        } else {
            return new Path(dir, fileName).toString();
        }
    }
}
