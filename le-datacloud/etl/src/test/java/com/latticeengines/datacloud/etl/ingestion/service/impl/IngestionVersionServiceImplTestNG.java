package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;

public class IngestionVersionServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(IngestionVersionServiceImplTestNG.class);

    @Inject
    private IngestionVersionService ingestionVersionService;

    @BeforeClass(groups = "functional")
    public void setup() {
        prepareCleanPod(IngestionVersionServiceImplTestNG.class.getSimpleName());
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        prepareCleanPod(IngestionVersionServiceImplTestNG.class.getSimpleName());
    }

    @Test(groups = "functional", enabled = true)
    public void testGetMostRecentVersions() {
        String[] versionArr = { "2019-06-30_00-00-00_UTC", "2019-07-07_00-00-00_UTC" };
        for (String version : versionArr) {
            String ingestionDir = hdfsPathBuilder.constructIngestionDir(IngestionNames.DNB_CASHESEED, version)
                    .toString();
            try {
                HdfsUtils.mkdir(yarnConfiguration, ingestionDir);
            } catch (IOException e) {
                throw new RuntimeException("Fail to create directory: " + ingestionDir, e);
            }
        }

        List<String> versions = ingestionVersionService
                .getMostRecentVersionsFromHdfs(IngestionNames.DNB_CASHESEED, 3);
        Collections.sort(versions);
        Assert.assertEquals(versions.toArray(), versionArr);

        versions = ingestionVersionService
                .getMostRecentVersionsFromHdfs(IngestionNames.DNB_CASHESEED, 1);
        Collections.sort(versions);
        Assert.assertEquals(versions.toArray(), new String[] { "2019-07-07_00-00-00_UTC" });
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
}
