package com.latticeengines.datacloudapi.engine.ingestion.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Triple;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.testframework.TestIngestionService;
import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

public class IngestionServiceTestNG extends PropDataEngineFunctionalTestNGBase {

    private static final String PREFIX = IngestionServiceTestNG.class.getSimpleName();
    private static final String FILE_NAME = "LE_SEED_OUTPUT_2016_08_003.OUT.gz";
    private static final String TEST_SUBMITTER = PropDataConstants.SCAN_SUBMITTER;
    private static final String VERSION = "2019-01-01_00-00-00_UTC";

    @Inject
    private TestIngestionService testIngestionService;

    @Inject
    private IngestionService ingestionService;

    @Inject
    private IngestionProgressService progressService;

    @Inject
    private IngestionProgressEntityMgr progressEntityMgr;

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    private Ingestion ingestion;

    @BeforeClass(groups = "functional")
    public void setup() {
        createIngestion();
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        deleteIngestion();
    }

    @Test(groups = "functional", dataProvider = "ProgressAppIds")
    public void testKillFailedProgresses(String appId) {
        IngestionProgress progress = progressService.createDraftProgress(ingestion, TEST_SUBMITTER, FILE_NAME,
                VERSION);
        progress.setApplicationId(appId);
        progress.setStatus(ProgressStatus.PROCESSING);
        progress = progressService.saveProgress(progress);
        Assert.assertNotNull(progress.getPid());
        ingestionService.killFailedProgresses();
        progress = progressEntityMgr.findProgress(progress);
        // Only valid ApplicationId which doesn't exist in RM is killed
        // Other invalid ApplicationIds just log exception in service,
        // expectation is they should not cause method to fail
        if (appId != null && appId.startsWith("application")) {
            Assert.assertEquals(progress.getStatus(), ProgressStatus.FAILED);
            Assert.assertEquals(progress.getRetries(), IngestionServiceImpl.ULTIMATE_RETRIES);
        }
    }

    @DataProvider(name = "ProgressAppIds")
    private Object[][] getProgressAppIds() {
        return new Object[][] { //
                // Valid ApplicationId format
                { "application_1490259718159_00001" }, //
                // Invalid ApplicationId format
                { "abcxyz" }, //
                // Empty ApplicationId
                { "" }, //
                // Blank ApplicationId
                { "   " }, //
                // Null ApplicationId
                { null }, //
        };
    }

    private void createIngestion() {
        ingestion = testIngestionService
                .createIngestions(
                        Collections.singletonList(Triple.of(PREFIX + "_Ingestion",
                                "{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":2,\"SftpDir\":\"/ingest_test/IngestionResourceDeploymentTestNG/DnB\",\"CheckVersion\":1,\"CheckStrategy\":\"ALL\",\"FileExtension\":\"OUT.gz\",\"FileNamePrefix\":\"LE_SEED_OUTPUT_\",\"FileNamePostfix\":\"(.*)\",\"FileTimestamp\":\"yyyy_MM\"}",
                                IngestionType.SFTP)))
                .get(0);
        Assert.assertNotNull(ingestion.getPid());
    }

    private void deleteIngestion() {
        ingestionEntityMgr.delete(ingestion);
    }
}
