package com.latticeengines.datacloudapi.engine.orchestration.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationProgressService;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationValidator;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;

@Component("orchestrationService")
public class OrchestrationServiceImpl implements OrchestrationService {

    private static Logger log = LoggerFactory.getLogger(OrchestrationServiceImpl.class);

    @Inject
    private OrchestrationEntityMgr orchestrationEntityMgr;

    @Inject
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;

    @Inject
    private OrchestrationProgressService orchestrationProgressService;

    @Inject
    private OrchestrationValidator orchestrationValidator;

    @Inject
    private IngestionService ingestionService;

    @Inject
    private SourceTransformationService sourceTransformationService;

    @Inject
    private PublicationService publicationService;

    @Inject
    private List<DataCloudEngineService> engineServices;

    private Map<DataCloudEngine, DataCloudEngineService> serviceMap;

    private static final String ORCHESTRATION = "Orchestration";

    @PostConstruct
    private void postConstruct() {
        serviceMap = new HashMap<>();
        for (DataCloudEngineService service : engineServices) {
            serviceMap.put(service.getEngine(), service);
        }
    }

    @Override
    public List<OrchestrationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        trigger();
        List<OrchestrationProgress> progresses = checkStatus();
        return process(progresses);
    }

    private void trigger() {
        List<Orchestration> orchs = orchestrationEntityMgr.findAll();
        List<OrchestrationProgress> progresses = new ArrayList<>();
        for (Orchestration orch : orchs) {
            List<String> triggeredVersions = new ArrayList<>();
            if (orchestrationValidator.isTriggered(orch, triggeredVersions)) {
                log.info(String.format("triggered orchestration: %s, triggered versions: %s", orch.toString(),
                        String.join(",", triggeredVersions)));
                progresses.addAll(orchestrationProgressService.createDraftProgresses(orch, triggeredVersions));
            }
        }
        orchestrationProgressEntityMgr.saveProgresses(progresses);
    }

    private List<OrchestrationProgress> checkStatus() {
        List<OrchestrationProgress> progresses = orchestrationProgressService.findProgressesToCheckStatus();
        if (progresses == null) {
            return new ArrayList<>();
        }
        for (OrchestrationProgress progress : progresses) {
            if (progress.getStatus() == ProgressStatus.NEW || progress.getStatus() == ProgressStatus.FAILED) {
                continue;
            }
            DataCloudEngineStage currentStage = progress.getCurrentStage();
            DataCloudEngineService engineService = serviceMap.get(currentStage.getEngine());
            if (engineService == null) {
                throw new UnsupportedOperationException(
                        String.format("Not support to execute orchestration pipeline from %s engine",
                                currentStage.getEngine().name()));
            }
            currentStage = engineService.findProgressAtVersion(currentStage);
            progress.setCurrentStage(currentStage);
        }
        return progresses;
    }

    private List<OrchestrationProgress> process(List<OrchestrationProgress> progresses) {
        for (OrchestrationProgress progress : progresses) {
            switch (progress.getStatus()) {
            case FAILED:
                retryFailedProgress(progress);
                break;
            case NEW:
                startNewProgress(progress);
                break;
            case PROCESSING:
                checkRunningProgress(progress);
                break;
            default:
                throw new RuntimeException(String.format("Unsupported status %s to process", progress.getStatus()));
            }
        }
        return progresses;
    }

    private OrchestrationProgress retryFailedProgress(OrchestrationProgress progress) {
        DataCloudEngineStage currentStage = progress.getCurrentStage();
        startJob(currentStage, progress.getHdfsPod());
        progress = orchestrationProgressService.updateSubmittedProgress(progress);
        return progress;
    }

    private OrchestrationProgress startNewProgress(OrchestrationProgress progress) {
        Orchestration orch = progress.getOrchestration();
        OrchestrationConfig config = orch.getConfig();
        DataCloudEngineStage currentStage = config.firstStage();
        currentStage.setVersion(progress.getVersion());
        startJob(currentStage, progress.getHdfsPod());
        progress.setCurrentStage(currentStage);
        progress = orchestrationProgressService.updateSubmittedProgress(progress);
        return progress;
    }

    private OrchestrationProgress checkRunningProgress(OrchestrationProgress progress) {
        DataCloudEngineStage currentStage = progress.getCurrentStage();
        switch (currentStage.getStatus()) {
        case FINISHED:
            Orchestration orch = progress.getOrchestration();
            OrchestrationConfig config = orch.getConfig();
            DataCloudEngineStage nextStage = config.nextStage(currentStage);
            if (nextStage != null) { // Proceed next stage
                nextStage.setStatus(ProgressStatus.NOTSTARTED);
                nextStage.setVersion(progress.getVersion());
                startJob(nextStage, progress.getHdfsPod());
                progress = orchestrationProgressService.updateProgress(progress).currentStage(nextStage).message(null)
                        .commit(true);
            } else { // Finish the pipeline
                progress = orchestrationProgressService.updateProgress(progress).currentStage(null)
                        .status(ProgressStatus.FINISHED).message(null).commit(true);
            }
            break;
        case FAILED:
            progress = orchestrationProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                    .message(currentStage.getMessage()).commit(true);
            break;
        default:
            Long startTime = progress.getLatestUpdateTime().getTime();
            Long currentTime = System.currentTimeMillis();
            if (currentTime - startTime > currentStage.getTimeout() * 60000) {
                progress = orchestrationProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                        .message("Timeout").commit(true);
            }
            break;
        }
        return progress;
    }

    private void startJob(DataCloudEngineStage stage, String hdfsPod) {
        switch (stage.getEngine()) {
        case INGESTION:
            startIngest(stage, hdfsPod);
            break;
        case TRANSFORMATION:
            startTransform(stage, hdfsPod);
            break;
        case PUBLICATION:
            startPublish(stage);
            break;
        default:
            throw new UnsupportedOperationException(
                    String.format("Unsupported engine type %s in orchestration pipeline", stage.getEngine().name()));
        }
    }

    private void startIngest(DataCloudEngineStage stage, String hdfsPod) {
        IngestionRequest request = new IngestionRequest();
        request.setSubmitter(ORCHESTRATION);
        request.setSourceVersion(stage.getVersion());
        ingestionService.ingest(stage.getEngineName(), request, hdfsPod, false);
    }

    private void startTransform(DataCloudEngineStage stage, String hdfsPod) {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(stage.getEngineName());
        request.setSubmitter(ORCHESTRATION);
        request.setVersion(stage.getVersion());
        request.setKeepTemp(true);
        sourceTransformationService.pipelineTransform(request, hdfsPod);
    }

    private void startPublish(DataCloudEngineStage stage) {
        PublicationRequest request = new PublicationRequest();
        request.setSubmitter(ORCHESTRATION);
        request.setSourceVersion(stage.getVersion());
        publicationService.kickoff(stage.getEngineName(), request);
    }

    @Override
    public DataCloudEngineStage getDataCloudEngineStatus(DataCloudEngineStage stage) {
        if (StringUtils.isBlank(stage.getEngineName()) || StringUtils.isBlank(stage.getEngine().name())
                || StringUtils.isBlank(stage.getVersion())) {
            throw new RuntimeException(
                    "Required fields are not populated. Expected fields are engineName, engine and version");
        }
        DataCloudEngineService engineService = serviceMap.get(stage.getEngine());
        if (engineService == null) {
            throw new UnsupportedOperationException(String
                    .format("Specified engine name %s is not supported.", stage.getEngine().name()));
        }
        DataCloudEngineStage status = engineService.findProgressAtVersion(stage);
        return status;
    }

}
