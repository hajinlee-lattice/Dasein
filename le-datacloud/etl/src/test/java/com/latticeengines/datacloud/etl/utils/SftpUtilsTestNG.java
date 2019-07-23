package com.latticeengines.datacloud.etl.utils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;

public class SftpUtilsTestNG extends DataCloudEtlFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SftpUtilsTestNG.class);

    @Value("${datacloud.test.sftp.host}")
    private String sftpHost;

    @Value("${datacloud.test.sftp.port}")
    private int sftpPort;

    @Value("${datacloud.test.sftp.username}")
    private String sftpUserName;

    @Value("${datacloud.test.sftp.password}")
    private String sftpPassword;

    private static final Calendar BOMBORA_CALENDAR = getBomboraCalendar();

    // Expected returned Bombora files based on files on SFTP and prepared SFTP
    // config and calendar
    private static final List<String> BOMBORA_FILES = Arrays.asList( //
            "/20190714/AllDomainsAllTopicsZips_20190714_1.csv.gz", //
            "/20190721/AllDomainsAllTopicsZips_20190721_1.csv.gz", //
            "/20190721/AllDomainsAllTopicsZips_20190721_2.csv.gz" //
    );

    private static final Calendar DNB_CALENDAR = getDnBCalendar();

    // Expected returned DnB files based on files on SFTP and prepared SFTP
    // config and calendar
    private static final List<String> DNB_FILES = Arrays.asList( //
            "LE_SEED_OUTPUT_2016_09_001.OUT.gz" //
    );

    @Test(groups = "functional")
    public void testGetFileNames() {
        SftpConfiguration config = getBomboraSftpConfig();
        String fileNamePattern = config.getFileNamePrefix() + "(.*)" + config.getFileNamePostfix()
                + config.getFileExtension();
        List<String> fileNames = SftpUtils.getFileNames(config, fileNamePattern, BOMBORA_CALENDAR);
        Collections.sort(fileNames);
        Assert.assertEquals(BOMBORA_FILES.toArray(new String[0]), fileNames.toArray(new String[0]),
                String.format("Expected Bombora files: %s, actual Bombora files: %s", String.join(",", BOMBORA_FILES),
                        String.join(",", fileNames)));

        config = getDnBSftpConfig();
        fileNamePattern = config.getFileNamePrefix() + "(.*)" + config.getFileNamePostfix() + config.getFileExtension();
        fileNames = SftpUtils.getFileNames(config, fileNamePattern, DNB_CALENDAR);
        Collections.sort(fileNames);
        Assert.assertEquals(
                DNB_FILES.toArray(new String[0]), fileNames.toArray(new String[0]), String.format("Expected DnB files: %s, actual DnB files: %s", String.join(",", DNB_FILES),
                        String.join(",", fileNames)));
    }

    private static Calendar getBomboraCalendar() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.AM_PM, Calendar.AM);
        calendar.set(Calendar.MONTH, Calendar.JULY);
        calendar.set(Calendar.DAY_OF_MONTH, 23);
        calendar.set(Calendar.YEAR, 2019);
        return calendar;
    }

    private SftpConfiguration getBomboraSftpConfig() {
        SftpConfiguration config = new SftpConfiguration();
        config.setSftpHost(sftpHost);
        config.setSftpPort(sftpPort);
        config.setSftpUserName(sftpUserName);
        config.setSftpPasswordEncrypted(sftpPassword);
        config.setSftpDir("ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip");

        config.setCheckStrategy(VersionCheckStrategy.WEEK);
        config.setCheckVersion(1);

        config.setHasSubFolder(true);
        config.setSubFolderTSPattern("yyyyMMdd");

        config.setFileNamePrefix("AllDomainsAllTopicsZips_");
        config.setFileNamePostfix("(.*)");
        config.setFileExtension("csv.gz");
        config.setFileTimestamp("yyyyMMdd");

        return config;
    }

    private static Calendar getDnBCalendar() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.AM_PM, Calendar.AM);
        calendar.set(Calendar.MONTH, Calendar.SEPTEMBER);
        calendar.set(Calendar.DAY_OF_MONTH, 30);
        calendar.set(Calendar.YEAR, 2016);
        return calendar;
    }

    private SftpConfiguration getDnBSftpConfig() {
        SftpConfiguration config = new SftpConfiguration();
        config.setSftpHost(sftpHost);
        config.setSftpPort(sftpPort);
        config.setSftpUserName(sftpUserName);
        config.setSftpPasswordEncrypted(sftpPassword);
        config.setSftpDir("/ingest_test/SftpUtilsTestNG/DnB/gets");

        config.setCheckStrategy(VersionCheckStrategy.MONTH);
        config.setCheckVersion(0);

        config.setHasSubFolder(false);

        config.setFileNamePrefix("LE_SEED_OUTPUT_");
        config.setFileNamePostfix("(.*)");
        config.setFileExtension("OUT.gz");
        config.setFileTimestamp("yyyy_MM");

        return config;
    }
}
