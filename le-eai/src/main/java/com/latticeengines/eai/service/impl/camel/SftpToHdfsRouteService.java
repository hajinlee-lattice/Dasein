package com.latticeengines.eai.service.impl.camel;

import java.io.File;

import javax.annotation.PostConstruct;

import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
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
import com.latticeengines.eai.service.CamelRouteService;

@Component("sftpToHdfsRoutService")
@Scope("prototype")
public class SftpToHdfsRouteService implements CamelRouteService<SftpToHdfsRouteConfiguration> {

    private static final String OPEN_SUFFIX = SftpToHdfsRouteConfiguration.OPEN_SUFFIX;
    private static final Log log = LogFactory.getLog(SftpToHdfsRouteService.class);

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
            throw new IllegalArgumentException("Unknown protocol from fs.defaultFS: "
                    + yarnConfiguration.get("fs.defaultFS"));
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

        final String sftpUrl = String.format("sftp://%s:%d%s?noop=true&fileName=%s&username=%s&password=%s",
                config.getSftpHost(), config.getSftpPort(), sftpDir, config.getFileName(),
                config.getSftpUserName(), CipherUtils.decrypt(config.getSftpPasswordEncrypted()));

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
                final String hdfsUrl = String.format(hdfsHostPort + "%s?openedSuffix=%s",
                        cleanDirPath(config.getHdfsDir()), OPEN_SUFFIX);
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
            switch (fsType) {
                case LOCAL:
                    return FileUtils.waitFor(new File(fullPath), 1);
                case HDFS:
                default:
                    if(HdfsUtils.fileExists(yarnConfiguration, fullPath)) {
                        log.info("Successfully downloaded a file of size " + checkDestFileSize(config) + "  bytes.");
                        return true;
                    } else {
                        return false;
                    }
            }
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
            sourceFileSize.set(null);
        }

        try {
            destFileSize.set(checkDestFileSize(config));
        } catch (Exception e) {
            // ignore
        }

        if (sourceFileSize.get() == 0) {
            throw new IllegalStateException("Source file size is zero.");
        }

        if (destFileSize.get() != null && sourceFileSize.get() != null) {
            progress.set(destFileSize.get().doubleValue() / sourceFileSize.get().doubleValue());
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
        try {
            String sftpUserName = config.getSftpUserName();
            String sftpHost = config.getSftpHost();
            Integer sftpPort = config.getSftpPort();
            String sftpPassword = CipherUtils.decrypt(config.getSftpPasswordEncrypted());
            String fileName = config.getFileName();

            JSch jsch = new JSch();
            Session session = jsch.getSession(sftpUserName, sftpHost, sftpPort);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(sftpPassword);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            SftpATTRS attrs = sftpChannel.stat(sftpChannel.pwd() + "/" + fileName);
            Long fileSize = attrs.getSize();

            sftpChannel.exit();
            session.disconnect();
            return fileSize;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }

    private Long checkDestFileSize(SftpToHdfsRouteConfiguration config) {
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

}
