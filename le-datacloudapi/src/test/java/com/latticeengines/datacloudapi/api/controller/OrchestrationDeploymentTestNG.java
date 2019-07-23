package com.latticeengines.datacloudapi.api.controller;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationProgressService;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

// dpltc deploy -a datacloudapi,workflowapi
public class OrchestrationDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrchestrationDeploymentTestNG.class);

    public final String POD_ID = this.getClass().getSimpleName();

    private static final String DNB_ORCHESTRATION = "DnBOrchestration_"
            + OrchestrationDeploymentTestNG.class.getSimpleName();
    private static final String DNB_INGESTION = "DnBIngestion_" + OrchestrationDeploymentTestNG.class.getSimpleName();
    private static final String DNB_TRANSFORMATION = "DnBCacheSeedPipeline";
    private static final String DNBRAW_TRANSFORMATION = "DnBCacheSeedRawPipeline";
    private static final String DNB_VERSION = "2017-01-01_00-00-00_UTC";
    private static final String DNB_FILE = "LE_SEED_OUTPUT_2017_01_052.OUT.gz";

    @Inject
    private OrchestrationEntityMgr orchestrationEntityMgr;
    @Inject
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;
    @Inject
    private IngestionEntityMgr ingestionEntityMgr;
    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;
    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;
    @Inject
    protected Configuration yarnConfiguration;
    @Inject
    private IngestionVersionService ingestionVersionService;
    @Inject
    private PipelineSource pipelineSource;
    @Inject
    private OrchestrationService orchService;
    @Inject
    private OrchestrationProgressService orchProgressService;
    @Inject
    private TransformationProgressEntityMgr transformationProgressEntityMgr;

    private List<Ingestion> ingestions = new ArrayList<>();
    private List<Orchestration> orchestrations = new ArrayList<>();

    // Name, ConfigStr
    @DataProvider(name = "Orchestrations")
    private static Object[][] getOrchestrations() {
        return new Object[][] { //
                { DNB_ORCHESTRATION,
                        String.format(
                                "{\"ClassName\":\"ExternalTriggerConfig\",\"PipelineConfig\":[{\"Engine\":\"TRANSFORMATION\",\"EngineName\":\"%s\",\"Timeout\":20},{\"Engine\":\"TRANSFORMATION\",\"EngineName\":\"%s\",\"Timeout\":20}],\"Engine\":\"INGESTION\",\"EngineName\":\"%s\",\"TriggerStrategy\":\"LATEST_VERSION\"}",
                                DNBRAW_TRANSFORMATION, DNB_TRANSFORMATION, DNB_INGESTION),
                        "DnBCacheSeed", "2017-01-01_00-00-00_UTC" }, //
        };
    }

    // IngestionName, Config, IngestionType
    private static Object[][] getIngestions() {
        return new Object[][] { { DNB_INGESTION, "{\"ClassName\":\"SftpConfiguration\"", IngestionType.SFTP }, //
        };
    }

    @BeforeClass(groups = "deployment", enabled = true)
    public void init() {
        prepareCleanPod(POD_ID);
        for (Object[] data : getOrchestrations()) {
            Orchestration orch = new Orchestration();
            orch.setName((String) data[0]);
            orch.setSchedularEnabled(true);
            orch.setMaxRetries(3);
            orch.setConfigStr((String) data[1]);
            orchestrationEntityMgr.save(orch);
            orchestrations.add(orchestrationEntityMgr.findByField("Name", orch.getName()));
        }
        for (Object[] data : getIngestions()) {
            Ingestion ingestion = new Ingestion();
            ingestion.setIngestionName((String) data[0]);
            ingestion.setConfig((String) data[1]);
            ingestion.setSchedularEnabled(false);
            ingestion.setNewJobRetryInterval(10000L);
            ingestion.setNewJobMaxRetry(1);
            ingestion.setIngestionType((IngestionType) data[2]);
            ingestionEntityMgr.save(ingestion);
            ingestions.add(ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));
        }
    }

    @AfterClass(groups = "deployment", enabled = true)
    public void tearDown() {
        for (Orchestration orch : orchestrations) {
            orchestrationEntityMgr.delete(orch);
        }
        for (Ingestion ingestion : ingestions) {
            ingestionEntityMgr.delete(ingestion);
        }
        List<TransformationProgress> transformProgresses = transformationProgressEntityMgr
                .findAllforPipeline(DNB_TRANSFORMATION);
        for (TransformationProgress progress : transformProgresses) {
            transformationProgressEntityMgr.deleteProgress(progress);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testOrchestration() {
        List<OrchestrationProgress> progresses = orchService.scan(POD_ID); // No job should be triggered
        Assert.assertEquals(progresses.size(), 0);
        uploadDnBIngestion();
        uploadDnBPipelineConfig();
        progresses = orchService.scan(POD_ID); // DnB pipeline is triggered
        int jobCount = getOrchestrations().length;
        Assert.assertEquals(progresses.size(), jobCount);
        Set<Long> pids = new HashSet<>();
        progresses.forEach(progress -> pids.add(progress.getPid()));
        long startTime = System.currentTimeMillis();
        // in order to test failed progress
        DataCloudEngineStage stageToFail = null;
        Long pidToFail = null;
        while (progresses.size() == jobCount && (System.currentTimeMillis() - startTime) <= 40 * 60000) {
            progresses = orchService.scan(POD_ID);
            progresses.forEach(progress -> log.info(progress.toString()));
            if (pidToFail == null) {
                pidToFail = progresses.get(0).getPid();
                stageToFail = progresses.get(0).getCurrentStage();
            }
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
            }
        }
        Assert.assertEquals(progresses.size(), 0);
        Object[][] orchs = getOrchestrations();
        for (Object[] orch : orchs) {
            String targetSource = (String) orch[2];
            Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(targetSource), (String) orch[3]);
        }
        for (Long pid : pids) {
            OrchestrationProgress progress = orchestrationProgressEntityMgr.findProgress(pid);
            Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
        }
        // Test failed progress
        OrchestrationProgress progressToFail = orchestrationProgressEntityMgr.findProgress(pidToFail);
        stageToFail.setStatus(ProgressStatus.FAILED);
        progressToFail = orchProgressService.updateProgress(progressToFail).status(ProgressStatus.FAILED)
                .currentStage(stageToFail).message(null).commit(true);
        progresses = orchService.scan(POD_ID); // failed progress is triggered
        Assert.assertEquals(progresses.size(), 1);
        while (progresses.size() == 1 && (System.currentTimeMillis() - startTime) <= 40 * 60000) {
            progresses = orchService.scan(POD_ID);
            progresses.forEach(progress -> log.info(progress.toString()));
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
            }
        }
        Assert.assertEquals(progresses.size(), 0);
        progressToFail = orchestrationProgressEntityMgr.findProgress(pidToFail);
        Assert.assertEquals(progressToFail.getStatus(), ProgressStatus.FINISHED);
    }

    private void uploadDnBIngestion() {
        InputStream baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + DNB_FILE);
        String targetPath = hdfsPathBuilder.constructIngestionDir(DNB_INGESTION, DNB_VERSION).append("/" + DNB_FILE)
                .toString();
        String successPath = hdfsPathBuilder.constructIngestionDir(DNB_INGESTION, DNB_VERSION).append("_SUCCESS")
                .toString();
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseSourceStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Ingestion dnbIngestion = ingestionEntityMgr.getIngestionByName(DNB_INGESTION);
        ingestionVersionService.updateCurrentVersion(dnbIngestion, DNB_VERSION);
    }

    private void uploadDnBPipelineConfig() {
        InputStream dnbRawStream = ClassLoader.getSystemResourceAsStream("sources/" + DNBRAW_TRANSFORMATION + ".json");
        InputStream dnbStream = ClassLoader.getSystemResourceAsStream("sources/" + DNB_TRANSFORMATION + ".json");
        String dnbRawTargetPath = hdfsPathBuilder.constructSourceDir(pipelineSource.getSourceName()).append("requests")
                .append(DNBRAW_TRANSFORMATION + ".json").toString();
        String dnbTargetPath = hdfsPathBuilder.constructSourceDir(pipelineSource.getSourceName()).append("requests")
                .append(DNB_TRANSFORMATION + ".json").toString();
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dnbRawStream, dnbRawTargetPath);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dnbStream, dnbTargetPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
