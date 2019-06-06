package com.latticeengines.datacloud.etl.orchestration.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationProgressService;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationValidator;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.ExternalTriggerConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.ExternalTriggerConfig.TriggerStrategy;
import com.latticeengines.domain.exposed.datacloud.orchestration.PredefinedScheduleConfig;

public class OrchestrationValidatorTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final String POD_ID = OrchestrationValidatorTestNG.class.getSimpleName();

    private static final String ORCHESTRATION_NAME = "Orchestration_" + OrchestrationValidatorTestNG.class.getSimpleName();
    private static final String INGESTION_NAME = "Ingestion_" + OrchestrationValidatorTestNG.class.getSimpleName();

    private Ingestion ingestion;
    private Orchestration orchestration;
    private OrchestrationProgress orchestrationprogress;

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @Inject
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;

    @Inject
    private OrchestrationEntityMgr orchestrationEntityMgr;

    @Inject
    private OrchestrationValidator orchestrationValidator;

    @Inject
    private OrchestrationProgressService orchestrationProgressService;

    @BeforeClass(groups = "functional")
    public void init() {
        prepareCleanPod(POD_ID);
        ingestion = new Ingestion();
        ingestion.setIngestionName(INGESTION_NAME);
        ingestion.setConfig(""); // Not needed in this test
        ingestion.setSchedularEnabled(false);
        ingestion.setNewJobRetryInterval(10000L);
        ingestion.setNewJobMaxRetry(1);
        ingestion.setIngestionType(IngestionType.SFTP);
        ingestionEntityMgr.save(ingestion);
        ingestion = ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName());

        orchestration = new Orchestration();
        orchestration.setName(ORCHESTRATION_NAME);
        orchestration.setConfigStr("");
        orchestration.setSchedularEnabled(false);
        orchestrationEntityMgr.save(orchestration);
        orchestration = orchestrationEntityMgr.findByField("Name", ORCHESTRATION_NAME);

    }

    @AfterClass(groups = "functional")
    public void destroy() {
        prepareCleanPod(POD_ID);
        ingestionEntityMgr.delete(ingestion);
        orchestrationEntityMgr.delete(orchestration);
    }
    
    @Test(groups = "functional", priority = 1)
    public void testScheduledTrigger() {
        List<String> triggeredVersions = new ArrayList<>();
        PredefinedScheduleConfig config = new PredefinedScheduleConfig();
        orchestration.setConfig(config);

        // disabled, not trigger
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        orchestration.setSchedularEnabled(true);

        // cron expression is empty, not trigger
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        config.setCronExpression("0 0 0 ? * * *");

        // prepare OrchestrationProgress
        orchestrationprogress = new OrchestrationProgress();
        orchestrationprogress.setHdfsPod(HdfsPodContext.getHdfsPodId());
        orchestrationprogress.setStatus(ProgressStatus.NEW);
        orchestrationprogress.setLatestUpdateTime(new Date());
        orchestrationprogress.setRetries(0);
        orchestrationprogress.setStartTime(new Date());
        orchestrationprogress.setTriggeredBy("");
        orchestrationprogress.setOrchestration(orchestration);
        orchestrationProgressEntityMgr.saveProgress(orchestrationprogress);

        // has job in progress, not triggered
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());


        // trigger at 0:00 every day when not job in progress
        orchestrationProgressService.updateProgress(orchestrationprogress).status(ProgressStatus.FINISHED)
                .commit(true);

        Assert.assertTrue(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertEquals(triggeredVersions.size(), 1);
        Assert.assertEquals(triggeredVersions.get(0),
                HdfsPathBuilder.dateFormat.format(new DateTime(new Date()).withTimeAtStartOfDay().toDate()));

        // already triggered, not repeatedly trigger
        OrchestrationProgress progress = orchestrationProgressService
                .createDraftProgresses(orchestration, triggeredVersions)
                .get(0);
        orchestrationProgressEntityMgr.saveProgress(progress);
        triggeredVersions.clear();
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

    }

    // Orchestration is enabled in testScheduledTrigger()
    @Test(groups = "functional", priority = 2)
    public void testExternalTrigger() {
        List<String> triggeredVersions = new ArrayList<>();
        ExternalTriggerConfig config = new ExternalTriggerConfig();
        config.setEngine(DataCloudEngine.INGESTION);
        config.setEngineName(INGESTION_NAME);
        config.setStrategy(TriggerStrategy.LATEST_VERSION);
        orchestration.setConfig(config);

        // ingestion doesn't have any version, not trigger
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        // create a version to ingestion, trigger with same version
        String version = "2019-01-01_00-00-00_UTC";
        ingestionVersionService.updateCurrentVersion(ingestion, version);
        Assert.assertTrue(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertEquals(triggeredVersions.size(), 1);
        Assert.assertEquals(triggeredVersions.get(0), version);

        // already triggered, not repeatedly trigger
        OrchestrationProgress progress = orchestrationProgressService
                .createDraftProgresses(orchestration, triggeredVersions).get(0);
        orchestrationProgressEntityMgr.saveProgress(progress);
        triggeredVersions.clear();
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        // update current version for ingestion, trigger with new version
        version = "2019-02-01_00-00-00_UTC";
        ingestionVersionService.updateCurrentVersion(ingestion, version);
        triggeredVersions.clear();
        Assert.assertTrue(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertEquals(triggeredVersions.size(), 1);
        Assert.assertEquals(triggeredVersions.get(0), version);
    }

}

