package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.ingestion.FileCheckStrategy;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.engine.ingestion.service.IngestionVersionService;
import com.latticeengines.propdata.engine.testframework.PropDataEngineFunctionalTestNGBase;

public class IngestionVersionServiceImplTestNG extends PropDataEngineFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(IngestionVersionServiceImplTestNG.class);

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @Test(groups = "functional", enabled = true)
    public void testGetMostRecentVersions() {
        List<String> versions = ingestionVersionService
                .getMostRecentVersionsFromHdfs(IngestionNames.DNB_CASHESEED, 3);
        log.info("Most recent 3 versions: ");
        for (String version : versions) {
            log.info(version);
        }
        versions = ingestionVersionService
                .getMostRecentVersionsFromHdfs(IngestionNames.DNB_CASHESEED, 1);
        log.info("Most recent 1 versions: ");
        for (String version : versions) {
            log.info(version);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testGetFileNamePattern() {
        String pattern = ingestionVersionService.getFileNamePattern("2016-07-01_00-00-00_UTC",
                "Bombora_Firehose_", "", ".csv.gz", "yyyyMMdd");
        Assert.assertEquals(pattern, "Bombora_Firehose_20160701.csv.gz");
        pattern = ingestionVersionService.getFileNamePattern("2016-07-01_00-00-00_UTC",
                "LE_SEED_OUTPUT_", "(.*)", ".OUT.gz", "yyyy_MM");
        Assert.assertEquals(pattern, "LE_SEED_OUTPUT_2016_07(.*).OUT.gz");
    }

    @Test(groups = "functional", enabled = true)
    public void testGetFileNamesOfMostRecentVersions() {
        List<String> fileNames = new ArrayList<String>(Arrays.asList(
                "Bombora_Firehose_20160809.csv.gz", "Bombora_Firehose_20160808.csv.gz",
                "Bombora_Firehose_20160807.csv.gz", "Bombora_Firehose_20160806.csv.gz",
                "Bombora_Firehose_20160805.csv.gz", "Bombora_Firehose_20160706.csv.gz",
                "Bombora_Firehose_20160705.csv.gz", "Bombora_Firehose_20160606.csv.gz"));
        List<String> result = ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames, 1,
                FileCheckStrategy.DAY, "yyyyMMdd");
        log.info("Check 1-day data:");
        for (String fileName : result) {
            log.info(fileName);
        }
        result = ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames, 3,
                FileCheckStrategy.DAY, "yyyyMMdd");
        log.info("Check 3-day data:");
        for (String fileName : result) {
            log.info(fileName);
        }
        result = ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames, 1,
                FileCheckStrategy.MONTH, "yyyyMMdd");
        log.info("Check 1-month data:");
        for (String fileName : result) {
            log.info(fileName);
        }
        result = ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames, 1,
                FileCheckStrategy.ALL, "yyyyMMdd");
        log.info("Check all data:");
        for (String fileName : result) {
            log.info(fileName);
        }
        fileNames = new ArrayList<String>(Arrays.asList("LE_SEED_OUTPUT_2016_07_TEST1.OUT.gz",
                "LE_SEED_OUTPUT_2016_07_TEST2.OUT.gz", "LE_SEED_OUTPUT_2016_06_TEST1.OUT.gz"));
        result = ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames, 1,
                FileCheckStrategy.MONTH, "yyyy_MM");
        log.info("Check 1-month data:");
        for (String fileName : result) {
            log.info(fileName);
        }
    }
}