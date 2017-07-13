package com.latticeengines.datacloudapi.api.controller;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
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
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.proxy.exposed.datacloudapi.OrchestrationProxy;

@Component
public class OrchestrationResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrchestrationResourceDeploymentTestNG.class);

    public final String POD_ID = this.getClass().getSimpleName();
    
    private static final String DNB_ORCHESTRATION = "DnB_Orchestration";
    private static final String DNB_INGESTION = "DnB_Ingestion";
    private static final String DNB_TRANSFORMATION = "DnB_Transformation";
    private static final String DNB_VERSION = "2017-01-01_00-00-00_UTC";
    private static final String DNB_FILE = "LE_SEED_OUTPUT_2017_01_052.OUT.gz";

    @Autowired
    private OrchestrationEntityMgr orchestrationEntityMgr;
    @Autowired
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;
    @Autowired
    private OrchestrationProgressService orchestrationProgressService;
    @Autowired
    private IngestionEntityMgr ingestionEntityMgr;
    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;
    @Autowired
    protected Configuration yarnConfiguration;
    @Autowired
    private IngestionVersionService ingestionVersionService;
    @Autowired
    private PipelineSource pipelineSource;
    @SuppressWarnings("unused")
    @Autowired
    private OrchestrationService orchService;
    @Autowired
    private OrchestrationProxy orchProxy;
    @Autowired
    private TransformationProgressEntityMgr transformationProgressEntityMgr;

    private List<Ingestion> ingestions = new ArrayList<>();
    private List<Orchestration> orchestrations = new ArrayList<>();

    // Name, ConfigStr
    @DataProvider(name = "Orchestrations")
    private static Object[][] getOrchestrations() {
        return new Object[][] { //
                { DNB_ORCHESTRATION,
                        String.format(
                                "{\"ClassName\":\"ExternalTriggerConfig\",\"PipelineConfig\":\"[{\\\"Engine\\\":\\\"TRANSFORMATION\\\",\\\"EngineName\\\":\\\"%s\\\",\\\"Timeout\\\":20}]\",\"Engine\":\"INGESTION\",\"EngineName\":\"%s\",\"TriggerStrategy\":\"LATEST_VERSION\"}",
                                DNB_TRANSFORMATION, DNB_INGESTION) }, //
        };
    }
    
    // IngestionName, Config, IngestionType
    private static Object[][] getIngestions() {
        return new Object[][] {
                { DNB_INGESTION,
                        "{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":2,\"SftpHost\":\"10.41.1.31\",\"SftpPort\":22,\"SftpUsername\":\"sftpdev\",\"SftpPassword\":\"KPpl2JWz+k79LWvYIKz6cA==\",\"SftpDir\":\"/ingest_test/dnb\",\"CheckVersion\":1,\"CheckStrategy\":\"ALL\",\"FileExtension\":\"OUT.gz\",\"FileNamePrefix\":\"LE_SEED_OUTPUT_\",\"FileNamePostfix\":\"(.*)\",\"FileTimestamp\":\"yyyy_MM\"}",
                        IngestionType.SFTP }, //
        };
    }

    @BeforeClass(groups = "deployment")
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

    @Test(groups = "deployment")
    public void testOrchestration() {
        List<OrchestrationProgress> progresses = orchProxy.scan(POD_ID); // no job should be triggered
        Assert.assertEquals(progresses.size(), 0);  
        uploadDnBIngestion();
        uploadDnBPipelineConfig();
        progresses = orchProxy.scan(POD_ID); // DnB pipeline is triggered
        Assert.assertEquals(progresses.size(), 1);
        Assert.assertEquals(orchProxy.scan(POD_ID).size(), 0); // no job should be triggered
        for (OrchestrationProgress progress : progresses) {
            log.info(String.format("Waiting for progress to finish: %s", progress.toString()));
            JobStatus status = jobService.waitFinalJobStatus(progress.getApplicationId(), 3600);
            Assert.assertEquals(status.getStatus(), FinalApplicationStatus.SUCCEEDED);
            OrchestrationProgress finished = orchestrationProgressEntityMgr.findProgress(progress);
            Assert.assertEquals(finished.getStatus(), ProgressStatus.FINISHED);
        }
        // retry for failed progresses
        for (OrchestrationProgress progress : progresses) {
            progress = orchestrationProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
        }
        progresses = orchProxy.scan(POD_ID); // DnB pipeline is triggered again
        Assert.assertEquals(progresses.size(), 1);
        Assert.assertEquals(orchProxy.scan(POD_ID).size(), 0); // no job should be triggered
        for (OrchestrationProgress progress : progresses) {
            log.info(String.format("Waiting for progress to finish: %s", progress.toString()));
            JobStatus status = jobService.waitFinalJobStatus(progress.getApplicationId(), 3600);
            Assert.assertEquals(status.getStatus(), FinalApplicationStatus.SUCCEEDED);
            OrchestrationProgress finished = orchestrationProgressEntityMgr.findProgress(progress);
            Assert.assertEquals(finished.getStatus(), ProgressStatus.FINISHED);
            Assert.assertEquals(finished.getRetries(), 1);
        }
        Assert.assertEquals(orchProxy.scan(POD_ID).size(), 0); // no job should be triggered
    }

    private void uploadDnBIngestion() {
        InputStream baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + DNB_FILE);
        String targetPath = hdfsPathBuilder.constructIngestionDir(DNB_INGESTION, DNB_VERSION).append("/" + DNB_FILE)
                .toString();
        String successPath = hdfsPathBuilder.constructIngestionDir(DNB_INGESTION, DNB_VERSION)
                .append("_SUCCESS").toString();
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
        InputStream baseSourceStream = ClassLoader.getSystemResourceAsStream("sources/" + DNB_TRANSFORMATION + ".json");
        String targetPath = hdfsPathBuilder.constructSourceDir(pipelineSource.getSourceName()).append("requests")
                .append(DNB_TRANSFORMATION + ".json").toString();
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseSourceStream, targetPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass(groups = "deployment")
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
}
