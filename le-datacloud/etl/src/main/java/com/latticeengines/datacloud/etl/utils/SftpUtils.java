package com.latticeengines.datacloud.etl.utils;

import java.io.File;
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

public final class SftpUtils {

    protected SftpUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(SftpUtils.class);

    /**
     * Check whether a file existed on SFTP
     *
     * TODO: Support sub-folder
     *
     * @param config
     * @param fileName
     * @return
     */
    public static boolean ifFileExists(@NotNull SftpConfiguration config, @NotNull String fileName) {
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
     * Get file list from SFTP which satisfies subfolder/file naming pattern and
     * version check strategy configured
     *
     * @param config:
     *            SFTP configuration
     * @return file list (relative path under SFTP root dir)
     */
    public static List<String> getFileList(SftpConfiguration config) {
        validateSftpConfig(config);
        return getFileList(config, null);
    }

    /**
     * For testing purpose, pass in Calendar instance instead of always using
     * current time base
     *
     * @param config:
     *            SFTP configuration
     * @param calendar:
     *            calendar to compare with timestamp
     * @return file list (relative path under SFTP root dir)
     */
    @VisibleForTesting
    static List<String> getFileList(@NotNull SftpConfiguration config, Calendar calendar) {
        try {
            log.info(String.format("Connecting to SFTP %s... ", config.getSftpHost()));

            JSch jsch = new JSch();
            Session session = jsch.getSession(config.getSftpUserName(), config.getSftpHost(), config.getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(config.getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + getSanityPath(config.getSftpDir(), true));

            List<String> scanDirs = getScanDirs(config, sftpChannel, calendar);
            List<String> scanFiles = getScanFiles(config, sftpChannel, calendar, scanDirs);

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

        if (!config.hasSubfolder()) {
            return Arrays.asList("");
        }
        Pattern pattern = Pattern.compile(config.getSubfolderRegexPattern());
        try {
            List<String> scanDirs = new ArrayList<>();
            Vector<ChannelSftp.LsEntry> paths = channel.ls(".");
            for (ChannelSftp.LsEntry path : paths) {
                Matcher matcher = pattern.matcher(path.getFilename());
                if (path.getAttrs().isDir() && matcher.find()) {
                    scanDirs.add(getSanityPath(path.getFilename(), true));
                }
            }
            return VersionUtils.getMostRecentVersionPaths(scanDirs, config.getCheckVersion(), config.getCheckStrategy(),
                    config.getSubfolderTSPattern(), null, calendar);
        } catch (SftpException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get file list under scanned directory which satisfies file name pattern.
     * If file is under sub-folder, append sub-folder before file name too. No
     * recursive searching.
     *
     * @param config:
     *            SFTP configuration
     * @param channel:
     *            SFTP connection channel
     * @param calendar:
     *            calendar to compare with timestamps
     * @param scanDirs:
     *            folders to scan files
     * @return
     */
    @SuppressWarnings("unchecked")
    private static List<String> getScanFiles(@NotNull SftpConfiguration config, @NotNull ChannelSftp channel,
            Calendar calendar, @NotNull List<String> scanDirs) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(channel);
        Preconditions.checkNotNull(scanDirs);
        Pattern pattern = Pattern.compile(config.getFileRegexPattern());

        List<String> scanFiles = new ArrayList<>();
        try {
            for (String dir : scanDirs) {
                dir = getSanityPath(dir, false);
                if (StringUtils.isBlank(dir)) {
                    dir = ".";
                }
                Vector<ChannelSftp.LsEntry> files = channel.ls(dir);
                for (int i = 0; i < files.size(); i++) {
                    ChannelSftp.LsEntry file = files.get(i);
                    if (file.getAttrs().isDir()) {
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
                config.getFileTSPattern(), null, calendar);
    }

    /**
     * Rename folder/file name on SFTP. Only support single layer path.
     *
     * @param config:
     *            SFTP configuration
     * @param oldPath:
     *            old folder/file name
     * @param newPath:
     *            new folder/file name
     */
    public static void renamePath(@NotNull SftpConfiguration config, @NotNull String oldPath,
            @NotNull String newPath) {
        validateSftpConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(oldPath));
        Preconditions.checkArgument(StringUtils.isNotBlank(newPath));
        Preconditions.checkArgument(!oldPath.contains(File.separator));
        Preconditions.checkArgument(!newPath.contains(File.separator));

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

            sftpChannel.rename(oldPath, newPath);

            sftpChannel.exit();
            session.disconnect();
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
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
