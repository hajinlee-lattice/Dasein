package com.latticeengines.propdata.engine.ingestion.entitymgr.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionService;
import com.latticeengines.propdata.engine.testframework.PropDataEngineFunctionalTestNGBase;

@Component
public class IngestionProgressEntityMgrImplTestNG extends PropDataEngineFunctionalTestNGBase {
    private static Log log = LogFactory.getLog(IngestionProgressEntityMgrImplTestNG.class);

    private static final String INGESTION_NAME = IngestionNames.BOMBORA_FIREHOSE;
    private static final String HDFS_POD = "FunctionalBomboraFireHose";
    private static final String FILE_NAME = "Bombora_Firehose_20160101.csv.gz";
    private static final String FAILED_FILE_NAME = "Bombora_Firehose_20160102.csv.gz";
    private static final String TEST_SUBMITTER = PropDataConstants.SCAN_SUBMITTER;

    @Autowired
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Autowired
    private IngestionService ingestionService;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    private Ingestion ingestion;

    private IngestionProgress progress;

    private IngestionProgress failedProgress;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ingestion = ingestionService.getIngestionByName(INGESTION_NAME);
        Assert.assertNotNull(ingestion);
        HdfsPodContext.changeHdfsPodId(HDFS_POD);
        progress = createProgess(ingestion);
        failedProgress = createFailedProgess(ingestion);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        ingestionProgressService.deleteProgress(progress);
        ingestionProgressService.deleteProgress(failedProgress);
    }

    @Test(groups = "functional", enabled = true)
    public void testIngestionProgress() throws JsonProcessingException {
        progress = ingestionProgressEntityMgr.saveProgress(progress);
        Assert.assertNotNull(progress.getPid(), "Failed to save ingestion progress");

        progress = ingestionProgressService.updateProgress(progress)
                .status(ProgressStatus.PROCESSING).commit(true);
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("PID", progress.getPid());
        fields.put("ApplicationId", progress.getApplicationId());
        List<IngestionProgress> progresses = ingestionProgressEntityMgr
                .getProgressesByField(fields);
        Assert.assertNotNull(progresses, "Failed to get ingestion progresses by field");
        Assert.assertNotEquals(progresses.isEmpty(), true,
                "Failed to get ingestion progresses by field");
        Assert.assertEquals(progresses.get(0).getStatus(), ProgressStatus.PROCESSING,
                "Failed to update ingestion progress status");

        log.info("Ingestion progress: " + progresses.get(0).toString());
        Ingestion ingestion = progresses.get(0).getIngestion();
        log.info("Ingestion configuration: " + ingestion.toString());

    }

    @Test(groups = "functional", enabled = true)
    public void testRetryFailedIngestionProgress() throws JsonProcessingException {
        failedProgress = ingestionProgressEntityMgr.saveProgress(failedProgress);
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.getRetryFailedProgresses();
        Assert.assertNotNull(progresses);
        Assert.assertEquals(progresses.size() > 0, true);
        for (IngestionProgress progress : progresses) {
            log.info("Failed progress: " + progress.toString());
        }
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "testIngestionProgress",
            "testRetryFailedIngestionProgress" })
    public void testIsDuplicateIngestionProgress() throws JsonProcessingException {
        IngestionProgress duplicateProgress = createProgess(ingestion);
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

    private IngestionProgress createProgess(Ingestion ingestion) {
        IngestionProgress progress = new IngestionProgress();
        progress.setIngestion(ingestion);
        progress.setSource(ingestionProgressService.constructSource(ingestion, FILE_NAME));
        progress.setDestination(
                ingestionProgressService.constructDestination(ingestion, FILE_NAME));
        progress.setHdfsPod(HDFS_POD);
        progress.setApplicationId(UUID.randomUUID().toString().toUpperCase());
        progress.setStartTime(new Date());
        progress.setLastestStatusUpdateTime(new Date());
        progress.setRetries(0);
        progress.setSize(Long.valueOf("1000"));
        progress.setStatus(ProgressStatus.NEW);
        progress.setTriggeredBy(TEST_SUBMITTER);
        return progress;
    }

    private IngestionProgress createFailedProgess(Ingestion ingestion) {
        IngestionProgress progress = new IngestionProgress();
        progress.setIngestion(ingestion);
        progress.setSource(ingestionProgressService.constructSource(ingestion, FAILED_FILE_NAME));
        progress.setDestination(
                ingestionProgressService.constructDestination(ingestion, FAILED_FILE_NAME));
        progress.setHdfsPod(HDFS_POD);
        progress.setApplicationId(UUID.randomUUID().toString().toUpperCase());
        progress.setStartTime(new Date());
        progress.setLastestStatusUpdateTime(new Date());
        progress.setRetries(0);
        progress.setSize(Long.valueOf("1000"));
        progress.setStatus(ProgressStatus.FAILED);
        progress.setTriggeredBy(TEST_SUBMITTER);
        return progress;
    }

}
