package com.latticeengines.datacloud.etl.orchestration.service.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import com.latticeengines.domain.exposed.datacloud.orchestration.ExternalTriggerWithScheduleConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.PredefinedScheduleConfig;

public class OrchestrationValidatorTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final String POD_ID = OrchestrationValidatorTestNG.class.getSimpleName();

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

    private List<Ingestion> ingestions = new ArrayList<>();
    private List<Orchestration> orchestrations = new ArrayList<>();

    @BeforeClass(groups = "functional")
    public void init() {
        prepareCleanPod(POD_ID);
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        prepareCleanPod(POD_ID);
        ingestions.forEach(ingestion -> {
            ingestionEntityMgr.delete(ingestion);
        });
        orchestrations.forEach(orchestration -> {
            orchestrationEntityMgr.delete(orchestration);
        });
    }
    
    @Test(groups = "functional")
    public void testScheduledTrigger() {
        Orchestration orchestration = createOrchestration(
                OrchestrationValidatorTestNG.class.getSimpleName() + "_TestScheduledTrigger");

        PredefinedScheduleConfig config = new PredefinedScheduleConfig();
        orchestration.setConfig(config);

        verifyDisabledSchedular(orchestration);
        verifyScheduledTrigger(orchestration);
    }

    @Test(groups = "functional")
    public void testExternalTrigger() {
        Orchestration orchestration = createOrchestration(
                OrchestrationValidatorTestNG.class.getSimpleName() + "_TestExternalTrigger");
        Ingestion ingestion = createIngestion(
                OrchestrationValidatorTestNG.class.getSimpleName() + "_TestExternalTrigger");

        ExternalTriggerConfig config = new ExternalTriggerConfig();
        config.setEngine(DataCloudEngine.INGESTION);
        config.setEngineName(ingestion.getIngestionName());
        config.setStrategy(TriggerStrategy.LATEST_VERSION);
        orchestration.setConfig(config);

        verifyDisabledSchedular(orchestration);
        verifyExternalTrigger(orchestration, ingestion);
    }

    @Test(groups = "functional")
    public void testExternalWithScheduleTrigger() {
        Orchestration orchestration = createOrchestration(
                OrchestrationValidatorTestNG.class.getSimpleName() + "_TestExternalWithScheduleTrigger");
        Ingestion ingestion = createIngestion(
                OrchestrationValidatorTestNG.class.getSimpleName() + "_TestExternalWithScheduleTrigger");

        ExternalTriggerWithScheduleConfig config = new ExternalTriggerWithScheduleConfig();
        ExternalTriggerConfig externalConfig = new ExternalTriggerConfig();
        externalConfig.setEngine(DataCloudEngine.INGESTION);
        externalConfig.setEngineName(ingestion.getIngestionName());
        externalConfig.setStrategy(TriggerStrategy.LATEST_VERSION);
        config.setExternalTriggerConfig(externalConfig);
        PredefinedScheduleConfig scheduleConfig = new PredefinedScheduleConfig();
        config.setScheduleConfig(scheduleConfig);
        orchestration.setConfig(config);

        verifyDisabledSchedular(orchestration);
        verifyExternalWithScheduleTrigger(orchestration, ingestion);
    }

    private void verifyDisabledSchedular(Orchestration orchestration) {
        orchestration.setSchedularEnabled(false);
        List<String> triggeredVersions = new ArrayList<>();

        // disabled, not trigger
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        orchestration.setSchedularEnabled(true);
    }

    private void verifyScheduledTrigger(Orchestration orchestration) {
        List<String> triggeredVersions = new ArrayList<>();
        PredefinedScheduleConfig config = (PredefinedScheduleConfig) orchestration.getConfig();

        // cron expression is empty, not trigger
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        config.setCronExpression("0 0 0 ? * * *");

        // prepare OrchestrationProgress
        OrchestrationProgress orchestrationprogress = createOrchestrationProgress(orchestration);

        // has job in progress, not triggered
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        // trigger at 0:00 every day when not job in progress
        orchestrationProgressService.updateProgress(orchestrationprogress).status(ProgressStatus.FINISHED).commit(true);

        Assert.assertTrue(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertEquals(triggeredVersions.size(), 1);
        Assert.assertEquals(triggeredVersions.get(0),
                HdfsPathBuilder.dateFormat.format(new DateTime(new Date()).withTimeAtStartOfDay().toDate()));

        // already triggered, not repeatedly trigger
        OrchestrationProgress progress = orchestrationProgressService
                .createDraftProgresses(orchestration, triggeredVersions).get(0);
        orchestrationProgressEntityMgr.saveProgress(progress);
        triggeredVersions.clear();
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());
    }

    private void verifyExternalTrigger(Orchestration orchestration, Ingestion ingestion) {
        List<String> triggeredVersions = new ArrayList<>();

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

    private void verifyExternalWithScheduleTrigger(Orchestration orchestration, Ingestion ingestion) {
        List<String> triggeredVersions = new ArrayList<>();
        ExternalTriggerWithScheduleConfig config = (ExternalTriggerWithScheduleConfig) orchestration.getConfig();

        // cron expression is empty, not trigger
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        config.getScheduleConfig().setCronExpression("0 0 0 ? * * *");

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

        // update current version for ingestion, but already triggered since
        // previous fire time of cron expression, not trigger
        version = "2019-02-01_00-00-00_UTC";
        ingestionVersionService.updateCurrentVersion(ingestion, version);
        triggeredVersions.clear();
        Assert.assertFalse(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertTrue(triggeredVersions.isEmpty());

        // update progress's start time to be ahead of previous fire time of
        // cron expression, will trigger
        try {
            orchestrationProgressService.updateProgress(progress)
                    .startTime(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01")) //
                    .commit(true);
        } catch (ParseException e) {
            throw new RuntimeException("Fail to update start time for orchestration progress", e);
        }
        triggeredVersions.clear();
        Assert.assertTrue(orchestrationValidator.isTriggered(orchestration, triggeredVersions));
        Assert.assertEquals(triggeredVersions.size(), 1);
        Assert.assertEquals(triggeredVersions.get(0), version);
    }

    private Ingestion createIngestion(String ingestionName) {
        Ingestion ingestion = new Ingestion();
        ingestion.setIngestionName(ingestionName);
        ingestion.setConfig(""); // Not needed in this test
        ingestion.setSchedularEnabled(false);
        ingestion.setNewJobRetryInterval(10000L);
        ingestion.setNewJobMaxRetry(1);
        ingestion.setIngestionType(IngestionType.SFTP);
        ingestionEntityMgr.save(ingestion);
        ingestion = ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName());
        ingestions.add(ingestion);
        return ingestion;
    }

    private Orchestration createOrchestration(String orchName) {
        Orchestration orchestration = new Orchestration();
        orchestration.setName(orchName);
        orchestration.setConfigStr("");
        orchestration.setSchedularEnabled(false);
        orchestrationEntityMgr.save(orchestration);
        orchestration = orchestrationEntityMgr.findByField("Name", orchName);
        orchestrations.add(orchestration);
        return orchestration;
    }

    private OrchestrationProgress createOrchestrationProgress(Orchestration orchestration) {
        OrchestrationProgress orchestrationprogress = new OrchestrationProgress();
        orchestrationprogress.setHdfsPod(HdfsPodContext.getHdfsPodId());
        orchestrationprogress.setStatus(ProgressStatus.NEW);
        orchestrationprogress.setLatestUpdateTime(new Date());
        orchestrationprogress.setRetries(0);
        orchestrationprogress.setStartTime(new Date());
        orchestrationprogress.setTriggeredBy("");
        orchestrationprogress.setOrchestration(orchestration);
        orchestrationProgressEntityMgr.saveProgress(orchestrationprogress);
        return orchestrationprogress;
    }

}

