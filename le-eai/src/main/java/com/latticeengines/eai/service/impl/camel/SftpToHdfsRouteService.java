package com.latticeengines.eai.service.impl.camel;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.CamelRouteService;

@Component("sftpToHdfsRoutService")
public class SftpToHdfsRouteService extends EaiRuntimeService<SftpToHdfsRouteConfiguration>
        implements CamelRouteService<SftpToHdfsRouteConfiguration> {

    private static final String OPEN_SUFFIX = SftpToHdfsRouteConfiguration.OPEN_SUFFIX;
    private static final String CAMEL_TEMP_FILE_SUFFIX = "inprogress";
    private static final String KNOWN_HOSTS_FILE = "./known_hosts";
    private static final Logger log = LoggerFactory.getLogger(SftpToHdfsRouteService.class);
    private static final Long timeout = TimeUnit.HOURS.toMillis(48);
    @Autowired
    private Configuration yarnConfiguration;

    private String hdfsHostPort;
    private FsType fsType;
    private ThreadLocal<Double> progress = new ThreadLocal<>();
    private ThreadLocal<Long> sourceFileSize = new ThreadLocal<>();
    private ThreadLocal<Long> destFileSize = new ThreadLocal<>();

    @PostConstruct
    private void postConstruct() {
        hdfsHostPort = yarnConfiguration.get("fs.defaultFS");

        if (StringUtils.isEmpty(hdfsHostPort)) {
            throw new IllegalArgumentException("fs.defaultFS in yarnConfiguration is empty.");
        }

        while (hdfsHostPort.endsWith("/") && !hdfsHostPort.endsWith("://")) {
            hdfsHostPort = hdfsHostPort.substring(0, hdfsHostPort.lastIndexOf("/"));
        }
        if (hdfsHostPort.startsWith("hdfs:")) {
            hdfsHostPort = hdfsHostPort.replace("hdfs:", "hdfs2:");
            fsType = FsType.HDFS;
            log.info("The file system defined in yarnConfiguration is hdfs: " + hdfsHostPort);
        } else if (hdfsHostPort.startsWith("file:")) {
            hdfsHostPort = "file:";
            fsType = FsType.LOCAL;
            log.info("The file system defined in yarnConfiguration is local.");
        } else {
            throw new IllegalArgumentException(
                    "Unknown protocol from fs.defaultFS: " + yarnConfiguration.get("fs.defaultFS"));
        }
    }

    @Override
    public RouteBuilder generateRoute(CamelRouteConfiguration camelRouteConfiguration) {
        SftpToHdfsRouteConfiguration config = (SftpToHdfsRouteConfiguration) camelRouteConfiguration;
        String sftpDir = config.getSftpDir();
        if (StringUtils.isEmpty(sftpDir)) {
            sftpDir = "/";
        }
        if (!sftpDir.startsWith("/")) {
            sftpDir = "/" + sftpDir;
        }
        deleteKnownHostsFile();
        createKnownHostsFile();
        String sftpUrl;
        try {
            sftpUrl = String.format("sftp://%s:%d%s?", config.getSftpHost(), config.getSftpPort(), sftpDir)
                    + URLEncoder.encode(String.format(
                            "noop=true&" //
                                    + "fileName=%s&" //
                                    + "username=%s&" //
                                    + "password=%s&" //
                                    + "stepwise=false&" //
                                    + "localWorkDirectory=%s&" //
                                    + "knownHostsFile=%s",
                            config.getFileName(), config.getSftpUserName(),
                            CipherUtils.decrypt(config.getSftpPasswordEncrypted()), getTempDirectory(),
                            KNOWN_HOSTS_FILE), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        log.info("SFTP URL: " + sftpUrl);
        switch (fsType) {
        case LOCAL:
            final String fileUrl = String.format("file:%s?tempFileName={file:name}.%s",
                    cleanDirPath(config.getHdfsDir()), OPEN_SUFFIX);
            log.info("Downloading file to " + fileUrl.substring(0, fileUrl.lastIndexOf("?")));
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from(sftpUrl).to(fileUrl);
                }
            };
        case HDFS:
        default:
            final String hdfsUrl = String.format(hdfsHostPort + "%s?openedSuffix=%s", cleanDirPath(config.getHdfsDir()),
                    OPEN_SUFFIX);
            log.info("Downloading file to " + hdfsUrl.substring(0, hdfsUrl.lastIndexOf("?")));
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from(sftpUrl).to(hdfsUrl);
                }
            };
        }
    }

    @Override
    public Boolean routeIsFinished(CamelRouteConfiguration camelRouteConfiguration) {
        try {
            SftpToHdfsRouteConfiguration config = (SftpToHdfsRouteConfiguration) camelRouteConfiguration;
            String fullPath = cleanDirPath(config.getHdfsDir()) + config.getFileName();
            boolean isFinished = false;
            switch (fsType) {
            case LOCAL:
                isFinished = FileUtils.waitFor(new File(fullPath), 1);
                break;
            case HDFS:
            default:
                if (HdfsUtils.fileExists(yarnConfiguration, fullPath)) {
                    log.info("Successfully downloaded a file of size " + checkDestFileSize(config) + "  bytes.");
                    isFinished = true;
                } else {
                    isFinished = false;
                }
            }
            if (isFinished) {
                deleteKnownHostsFile();
            }
            return isFinished;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Double getProgress(CamelRouteConfiguration camelRouteConfiguration) {
        SftpToHdfsRouteConfiguration config = (SftpToHdfsRouteConfiguration) camelRouteConfiguration;

        if (progress.get() == null) {
            progress.set(0.0);
        }

        try {
            if (sourceFileSize.get() == null) {
                sourceFileSize.set(checkSourceFileSize(config));
            }
        } catch (Exception e) {
            log.error("Error in checking source file size: " + e.getMessage(), e);
            sourceFileSize.set(null);
        }

        try {
            destFileSize.set(checkDestFileSize(config));
        } catch (Exception e) {
            // ignore
        }

        if (sourceFileSize.get() == null || sourceFileSize.get() == 0) {
            throw new IllegalStateException("Source file size is zero.");
        }

        if (destFileSize.get() != null && sourceFileSize.get() != null) {
            Double percentage = destFileSize.get().doubleValue() / sourceFileSize.get().doubleValue();
            // keep only two decimal digit
            percentage = Math.round(percentage * 100.0) / 100.0;
            progress.set(percentage);
        }

        return progress.get();
    }

    private String cleanDirPath(String hdfsDir) {
        while (hdfsDir.endsWith("/")) {
            hdfsDir = hdfsDir.substring(0, hdfsDir.lastIndexOf("/"));
        }
        if (!hdfsDir.startsWith("/")) {
            hdfsDir = "/" + hdfsDir;
        }
        return hdfsDir + "/";
    }

    private enum FsType {
        HDFS, LOCAL
    }

    private Long checkSourceFileSize(SftpToHdfsRouteConfiguration config) {
        String path = "";
        try {
            String sftpUserName = config.getSftpUserName();
            String sftpHost = config.getSftpHost();
            Integer sftpPort = config.getSftpPort();
            String sftpPassword = CipherUtils.decrypt(config.getSftpPasswordEncrypted());
            String fileName = config.getFileName();
            String sftpDir = config.getSftpDir();
            if (StringUtils.isEmpty(sftpDir)) {
                sftpDir = "/";
            }
            if (!sftpDir.startsWith("/")) {
                sftpDir = "/" + sftpDir;
            }

            JSch jsch = new JSch();
            Session session = jsch.getSession(sftpUserName, sftpHost, sftpPort);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("stepwise", Boolean.FALSE.toString());
            session.setPassword(sftpPassword);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            path = sftpChannel.pwd() + sftpDir + "/" + fileName;

            SftpATTRS attrs = sftpChannel.stat(path);
            Long fileSize = attrs.getSize();

            sftpChannel.exit();
            session.disconnect();
            return fileSize;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Long checkDestFileSize(SftpToHdfsRouteConfiguration config) {
        try {
            return doLocalFileBasedCheck(config);
        } catch (Exception ex) {
            return doHdfsBasedCheck(config);
        }
    }

    private Long doHdfsBasedCheck(SftpToHdfsRouteConfiguration config) {
        try {
            String fileName = config.getFileName() + "." + OPEN_SUFFIX;
            String hdfsDir = cleanDirPath(config.getHdfsDir());
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir + fileName)) {
                return HdfsUtils.getFileSize(yarnConfiguration, hdfsDir + fileName);
            } else if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir + config.getFileName())) {
                return HdfsUtils.getFileSize(yarnConfiguration, hdfsDir + config.getFileName());
            } else {
                return 0L;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Long doLocalFileBasedCheck(SftpToHdfsRouteConfiguration config) {
        String fileName = config.getFileName() + "." + CAMEL_TEMP_FILE_SUFFIX;
        String localTempDir = getTempDirectory();
        File tempFile = new File(localTempDir + File.separator + fileName);
        if (tempFile.exists()) {
            log.info("Downloading to local file: " + tempFile.getAbsolutePath());
            return tempFile.length();
        } else {
            throw new RuntimeException("Could not find local temp file: " + fileName);
        }
    }

    private String getTempDirectory() {
        // use current directory as it is already set as per
        // yarn.nodemanager.local-dirs configuration
        return ".";
    }

    private void createKnownHostsFile() {
        // use current directory as it is already set as per
        // yarn.nodemanager.local-dirs configuration
        File knownHostsFile = new File(KNOWN_HOSTS_FILE);
        try {
            FileUtils.touch(knownHostsFile);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create the temporary known hosts file.", e);
        }
    }

    private void deleteKnownHostsFile() {
        FileUtils.deleteQuietly(new File(KNOWN_HOSTS_FILE));
    }

    @Override
    public void invoke(SftpToHdfsRouteConfiguration camelRouteConfig) {
        if (camelRouteConfig instanceof SftpToHdfsRouteConfiguration) {
            CamelContext camelContext = new DefaultCamelContext();

            RouteBuilder route = generateRoute(camelRouteConfig);
            try {
                camelContext.addRoutes(route);
                camelContext.start();
                waitForRouteToFinish(camelRouteConfig);
                camelContext.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } else {
            throw new UnsupportedOperationException(
                    camelRouteConfig.getClass().getSimpleName() + " has not been implemented yet.");
        }
    }

    private void waitForRouteToFinish(CamelRouteConfiguration camelRouteConfiguration) {
        long startTime = System.currentTimeMillis();
        int errorTimes = 0;
        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                if (routeIsFinished(camelRouteConfiguration)) {
                    setProgress(0.95f);
                    return;
                } else {
                    String msg = "Waiting for the camel route to finish";
                    Double progress = getProgress(camelRouteConfiguration);
                    if (progress != null) {
                        setProgress(progress.floatValue());
                        msg += ": " + progress * 100 + "%";
                    }
                    log.info(msg);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                if (++errorTimes >= 10) {
                    throw new RuntimeException("Max error times exceeded: encountered " + errorTimes + " errors.", e);
                }
            } finally {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    // ignore
                }
            }
        }
    }

}
