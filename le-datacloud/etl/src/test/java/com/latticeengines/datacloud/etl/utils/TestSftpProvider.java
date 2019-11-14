package com.latticeengines.datacloud.etl.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service to provide SFTP info for datacloud tests
 */
@Component("testSftpProvider")
public class TestSftpProvider {

    @Value("${datacloud.test.sftp.host}")
    private String sftpHost;

    @Value("${datacloud.test.sftp.port}")
    private int sftpPort;

    @Value("${datacloud.test.sftp.username}")
    private String sftpUserName;

    @Value("${datacloud.test.sftp.password}")
    private String sftpPassword;

    @Value("${datacloud.bombora.sftp.host}")
    private String bomboraSftpHost;

    @Value("${datacloud.bombora.sftp.port}")
    private int bomboraSftpPort;

    @Value("${datacloud.bombora.sftp.user}")
    private String bomboraSftpUsername;

    @Value("${datacloud.bombora.sftp.password}")
    private String bomboraSftpPassword;

    public String getSftpHost() {
        return sftpHost;
    }

    public int getSftpPort() {
        return sftpPort;
    }

    public String getSftpUserName() {
        return sftpUserName;
    }

    public String getSftpPassword() {
        return sftpPassword;
    }

    public String getBomboraSftpHost() {
        return bomboraSftpHost;
    }

    public int getBomboraSftpPort() {
        return bomboraSftpPort;
    }

    public String getBomboraSftpUsername() {
        return bomboraSftpUsername;
    }

    public String getBomboraSftpPassword() {
        return bomboraSftpPassword;
    }
}
