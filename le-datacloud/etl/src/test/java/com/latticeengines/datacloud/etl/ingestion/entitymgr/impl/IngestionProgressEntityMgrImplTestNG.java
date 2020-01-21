package com.latticeengines.datacloud.etl.ingestion.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

@Component
public class IngestionProgressEntityMgrImplTestNG extends DataCloudEtlFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(IngestionProgressEntityMgrImplTestNG.class);

    private static final String INGESTION_NAME = "TestBomboraFirehose";
    private static final String HDFS_POD = "FunctionalBomboraFireHose";
    private static final String FILE_NAME = "Bombora_Firehose_20160101.csv.gz";
    private static final String FAILED_FILE_NAME = "Bombora_Firehose_20160102.csv.gz";
    private static final String TEST_SUBMITTER = PropDataConstants.SCAN_SUBMITTER;

    @Inject
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    @Inject
    private IngestionProgressService ingestionProgressService;

    private Ingestion ingestion;

    private IngestionProgress progress;

    private IngestionProgress failedProgress;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ingestion = createIngestion();
        Assert.assertNotNull(ingestion);
        HdfsPodContext.changeHdfsPodId(HDFS_POD);
        progress = ingestionProgressService.createDraftProgress(ingestion, TEST_SUBMITTER, FILE_NAME, null);
        progress.setApplicationId(UUID.randomUUID().toString().toUpperCase());
        failedProgress = ingestionProgressService.createDraftProgress(ingestion, TEST_SUBMITTER, FAILED_FILE_NAME,
                null);
        failedProgress.setApplicationId(UUID.randomUUID().toString().toUpperCase());
        failedProgress.setStatus(ProgressStatus.FAILED);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        ingestionEntityMgr.delete(ingestion);
    }

    @Test(groups = "functional")
    public void testIngestionProgress() {
        progress = ingestionProgressEntityMgr.saveProgress(progress);
        Assert.assertNotNull(progress.getPid(), "Failed to save ingestion progress");

        progress = ingestionProgressService.updateProgress(progress)
                .status(ProgressStatus.PROCESSING).commit(true);
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("PID", progress.getPid());
        fields.put("ApplicationId", progress.getApplicationId());
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        Assert.assertNotNull(progresses, "Failed to get ingestion progresses by field");
        Assert.assertNotEquals(progresses.isEmpty(), true,
                "Failed to get ingestion progresses by field");
        Assert.assertEquals(progresses.get(0).getStatus(), ProgressStatus.PROCESSING,
                "Failed to update ingestion progress status");

        log.info("Ingestion progress: " + progresses.get(0).toString());
        Ingestion ingestion = progresses.get(0).getIngestion();
        log.info("Ingestion configuration: " + ingestion.toString());

    }

    @Test(groups = "functional")
    public void testRetryFailedIngestionProgress() {
        failedProgress = ingestionProgressEntityMgr.saveProgress(failedProgress);
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findRetryFailedProgresses();
        Assert.assertNotNull(progresses);
        Assert.assertTrue(progresses.size() > 0);
        for (IngestionProgress progress : progresses) {
            log.info("Failed progress: " + progress.toString());
        }
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "testIngestionProgress",
            "testRetryFailedIngestionProgress" })
    public void testIsDuplicateIngestionProgress() throws JsonProcessingException {
        IngestionProgress duplicateProgress = ingestionProgressService.createDraftProgress(ingestion, TEST_SUBMITTER,
                FILE_NAME, null);
        duplicateProgress.setApplicationId(UUID.randomUUID().toString().toUpperCase());
        Assert.assertTrue(ingestionProgressEntityMgr.isDuplicateProgress(duplicateProgress));
        progress.setStatus(ProgressStatus.FAILED);
        ingestionProgressEntityMgr.saveProgress(progress);
        Assert.assertTrue(ingestionProgressEntityMgr.isDuplicateProgress(duplicateProgress));
        progress.setRetries(ingestion.getNewJobMaxRetry() + 1);
        ingestionProgressEntityMgr.saveProgress(progress);
        Assert.assertTrue(!ingestionProgressEntityMgr.isDuplicateProgress(duplicateProgress));
        progress.setStatus(ProgressStatus.FINISHED);
        ingestionProgressEntityMgr.saveProgress(progress);
        Assert.assertTrue(!ingestionProgressEntityMgr.isDuplicateProgress(duplicateProgress));
    }

    private Ingestion createIngestion() {
        Ingestion ingestion = new Ingestion();
        ingestion.setIngestionName(INGESTION_NAME);
        ingestion.setSchedularEnabled(false);
        ingestion.setNewJobMaxRetry(3);
        ingestion.setNewJobRetryInterval(10000L);
        ingestion.setIngestionType(IngestionType.SFTP);
        SftpConfiguration sftpConfig = new SftpConfiguration();
        sftpConfig.setFileRegexPattern("Bombora_Firehose_(.+).csv.gz");
        sftpConfig.setFileTSPattern("yyyyMMdd");
        ingestion.setProviderConfiguration(sftpConfig);
        ingestionEntityMgr.save(ingestion);
        return ingestionEntityMgr.getIngestionByName(INGESTION_NAME);
    }

}
