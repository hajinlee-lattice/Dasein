package com.latticeengines.datacloud.etl.utils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;

/**
 * This test requires setup some files on the sftp server:
 *
 * 1.
 * ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip/20200908/AllDomainsAllTopicsZips_20190714_1.csv.gz
 * 2.
 * ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip/20200915/AllDomainsAllTopicsZips_20190721_1.csv.gz
 * 3.
 * ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip/20200915/AllDomainsAllTopicsZips_20190721_2.csv.gz
 * 4. ingest_test/SftpUtilsTestNG/DnB/gets/LE_SEED_OUTPUT_2019_05_001.OUT.gz
 *
 * The files can be found in
 * s3://latticeengines-test-artifacts/le-datacloud/etl-ingestion/1/
 */
public class SftpUtilsTestNG extends DataCloudEtlFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SftpUtilsTestNG.class);

    @Inject
    private TestSftpProvider sftpProvider;

    private static final Calendar BOMBORA_CALENDAR = getBomboraCalendar();

    // Expected returned Bombora files based on files on SFTP and prepared SFTP
    // config and calendar
    private static final List<String> BOMBORA_FILES = Arrays.asList( //
            "20210114/AllDomainsAllTopicsZips_20190714_1.csv.gz", //
            "20210121/AllDomainsAllTopicsZips_20190721_1.csv.gz", //
            "20210121/AllDomainsAllTopicsZips_20190721_2.csv.gz" //
    );

    private static final Calendar DNB_CALENDAR = getDnBCalendar();

    // Expected returned DnB files based on files on SFTP and prepared SFTP
    // config and calendar
    private static final List<String> DNB_FILES = Arrays.asList( //
            "LE_SEED_OUTPUT_2019_05_001.OUT.gz" //
    );

    @Test(groups = "functional")
    public void testGetFileNames() {
        SftpConfiguration config = getTestBomboraSftpConfig1();
        List<String> fileNames = SftpUtils.getFileList(config, BOMBORA_CALENDAR);
        Collections.sort(fileNames);
        Assert.assertEquals(BOMBORA_FILES.toArray(new String[0]), fileNames.toArray(new String[0]),
                String.format("Expected Bombora files: %s, actual Bombora files: %s", String.join(",", BOMBORA_FILES),
                        String.join(",", fileNames)));

        config = getTestBomboraSftpConfig2();
        fileNames = SftpUtils.getFileList(config, BOMBORA_CALENDAR);
        Collections.sort(fileNames);
        Assert.assertEquals(BOMBORA_FILES.toArray(new String[0]), fileNames.toArray(new String[0]),
                String.format("Expected Bombora files: %s, actual Bombora files: %s", String.join(",", BOMBORA_FILES),
                        String.join(",", fileNames)));

        config = getTestDnBSftpConfig();
        fileNames = SftpUtils.getFileList(config, DNB_CALENDAR);
        Collections.sort(fileNames);
        Assert.assertEquals(DNB_FILES.toArray(new String[0]), fileNames.toArray(new String[0]),
                String.format("Expected DnB files: %s, actual DnB files: %s", String.join(",", DNB_FILES),
                        String.join(",", fileNames)));
    }

    @Test(groups = "functional", enabled = false)
    public void testBomboraSFTP() {
        SftpConfiguration config = getBomboraSftpConfig();
        List<String> fileNames = SftpUtils.getFileList(config, null);
        // Just verify we are able to get file list from real Bombora SFTP. Data
        // correctness verification is covered in testGetFileNames() with
        // testing SFTP
        Assert.assertTrue(CollectionUtils.isNotEmpty(fileNames));
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

    private SftpConfiguration getTestBomboraSftpConfig1() {
        SftpConfiguration config = new SftpConfiguration();
        config.setSftpHost(sftpProvider.getSftpHost());
        config.setSftpPort(sftpProvider.getSftpPort());
        config.setSftpUserName(sftpProvider.getSftpUserName());
        config.setSftpPasswordEncrypted(sftpProvider.getSftpPassword());
        config.setSftpDir("/home/sftpdev/ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip");

        config.setCheckStrategy(VersionCheckStrategy.WEEK);
        config.setCheckVersion(1);

        config.setHasSubfolder(true);
        config.setSubfolderTSPattern("yyyyMMdd");
        config.setFileRegexPattern("AllDomainsAllTopicsZips_(.*).csv.gz");
        config.setFileTSPattern("yyyyMMdd");

        return config;
    }

    private SftpConfiguration getTestBomboraSftpConfig2() {
        SftpConfiguration config = getTestBomboraSftpConfig1();
        config.setSubfolderRegexPattern("\\d{8}");
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

    private SftpConfiguration getTestDnBSftpConfig() {
        SftpConfiguration config = new SftpConfiguration();
        config.setSftpHost(sftpProvider.getSftpHost());
        config.setSftpPort(sftpProvider.getSftpPort());
        config.setSftpUserName(sftpProvider.getSftpUserName());
        config.setSftpPasswordEncrypted(sftpProvider.getSftpPassword());
        config.setSftpDir("/home/sftpdev/ingest_test/SftpUtilsTestNG/DnB/gets");

        config.setCheckStrategy(VersionCheckStrategy.MONTH);
        config.setCheckVersion(0);

        config.setHasSubfolder(false);
        config.setFileRegexPattern("LE_SEED_OUTPUT_(.*).OUT.gz");
        config.setFileTSPattern("yyyy_MM");

        return config;
    }

    private SftpConfiguration getBomboraSftpConfig() {
        SftpConfiguration config = new SftpConfiguration();
        config.setSftpHost(sftpProvider.getBomboraSftpHost());
        config.setSftpPort(sftpProvider.getBomboraSftpPort());
        config.setSftpUserName(sftpProvider.getBomboraSftpUsername());
        config.setSftpPasswordEncrypted(sftpProvider.getBomboraSftpPassword());
        config.setSftpDir("/home/sftpdev/ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip");

        config.setCheckStrategy(VersionCheckStrategy.WEEK);
        config.setCheckVersion(1);

        config.setHasSubfolder(true);
        config.setSubfolderTSPattern("yyyyMMdd");
        config.setFileRegexPattern("AllDomainsAllTopicsZips_(.*).csv.gz");
        config.setFileTSPattern("yyyyMMdd");

        return config;
    }
}
