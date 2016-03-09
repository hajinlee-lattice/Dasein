package com.latticeengines.eai.service.impl.camel;

import java.io.InputStream;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

@Component("sftpToHdfsRouteServiceTestNG")
public class SftpToHdfsRouteServiceTestNG extends EaiFunctionalTestNGBase {

    private static final String hdfsDir = "/tmp/sftp2hdfsfunctional";

    private static final String sftpHost = "10.41.1.31";
    private static final int sftpPort = 22;
    private static final String sftpUserName = "sftpdev";
    private static final String sftpPassword = "welcome";
    private static final String fileName = "alexa.csv.gz";

    @Autowired
    private SftpToHdfsRouteService sftpToHdfsRouteService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanup();
    }

    @Test(groups = "functional")
    public void testDownloadFromSftp() throws Exception {
        SftpToHdfsRouteConfiguration configuration = getRouteConfiguration();
        RouteBuilder route = sftpToHdfsRouteService.generateRoute(configuration);

        CamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(route);
        camelContext.start();
        boolean downloaded = waitForFileToBeDownloaded();
        camelContext.stop();

        Assert.assertTrue(downloaded, "Did not find the file to be downloaded in hdfs.");
    }

    public void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, hdfsDir);
        uploadTestFileIfNotExists(fileName);
    }

    public boolean uploadTestFileIfNotExists(String fileName) {
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(sftpUserName, sftpHost, sftpPort);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(sftpPassword);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            try {
                sftpChannel.stat(sftpChannel.pwd() + "/" + fileName);
            } catch (SftpException e) {
                if (e.getMessage().contains("No such file")) {
                    log.info("Test file " + fileName + " does not exists. Uploading ...");
                    InputStream inputStream = Thread.currentThread().getContextClassLoader()
                            .getResourceAsStream("com/latticeengines/eai/service/impl/camel/" + fileName);
                    sftpChannel.put(inputStream, sftpChannel.pwd() + "/" + fileName);
                } else {
                    throw new RuntimeException(e);
                }
            }

            sftpChannel.exit();
            session.disconnect();
            return true;
        } catch (JSchException|SftpException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean waitForFileToBeDownloaded() {
        Long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir + "/" + fileName)) {
                    return true;
                }
                Thread.sleep(1000L);
            } catch (Exception e) {
                //ignore
            }
        }
        return false;
    }

    public SftpToHdfsRouteConfiguration getRouteConfiguration() {
        SftpToHdfsRouteConfiguration configuration = new SftpToHdfsRouteConfiguration();
        configuration.setFileName(fileName);
        configuration.setSftpHost(sftpHost);
        configuration.setSftpPort(sftpPort);
        configuration.setHdfsDir(hdfsDir);
        configuration.setSftpUserName(sftpUserName);
        configuration.setSftpPasswordEncrypted(CipherUtils.encrypt(sftpPassword).replace("\r","").replace("\n",""));
        return configuration;
    }

}
