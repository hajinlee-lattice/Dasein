package com.latticeengines.datacloud.workflow.engine.steps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationProgressService;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationPipelineStep;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.OrchestrationStepConfig;
import com.latticeengines.proxy.exposed.datacloudapi.IngestionProxy;
import com.latticeengines.proxy.exposed.datacloudapi.PublicationProxy;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("orchestrationStep")
@Scope("prototype")
public class OrchestrationStep extends BaseWorkflowStep<OrchestrationStepConfig> {
    private static final Log log = LogFactory.getLog(OrchestrationStep.class);

    private Orchestration orch;

    private OrchestrationConfig config;

    private OrchestrationProgress progress;

    @Autowired
    private OrchestrationProgressService orchestrationProgressService;

    @Autowired
    private IngestionProxy ingestProxy;

    @Autowired
    private TransformationProxy transformProxy;

    @Autowired
    private PublicationProxy publishProxy;

    @Autowired
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
    public void execute() {
        try {
            log.info("Start to execute orchestration pipeline");
            progress = getConfiguration().getOrchestrationProgress();
            HdfsPodContext.changeHdfsPodId(progress.getHdfsPod());
            orch = getConfiguration().getOrchestration();
            config = getConfiguration().getOrchestrationConfig();
            orch.setConfig(config);
            progress.setOrchestration(orch);
            log.info(String.format("Pipeline configuration: %s", config.getPipelineConfig()));
            OrchestrationPipelineStep step = config.firstStep();
            while (step != null) {
                execPipelineStep(step);
                step = config.nextStep(step);
            }
            progress = orchestrationProgressService.updateProgress(progress).currentStage(null)
                    .status(ProgressStatus.FINISHED).commit(true);
            log.info("Finished orchestration pipeline");
        } catch (Exception e) {
            failByException(e);
        }
    }

    private void execPipelineStep(OrchestrationPipelineStep step) {
        log.info(String.format("Current pipeline step: %s", step.toString()));
        progress = orchestrationProgressService.updateProgress(progress).currentStage(step.getEngine()).commit(true);
        DataCloudEngineService engineService = serviceMap.get(step.getEngine());
        if (engineService == null) {
            throw new UnsupportedOperationException(String
                    .format("Not support to execute orchestration pipeline from %s engine", step.getEngine().name()));
        }
        EngineProgress engineProgress = engineService.findProgressAtVersion(step.getEngineName(),
                progress.getVersion());
        if (engineProgress.getStatus() == ProgressStatus.FINISHED) {
            log.info(String.format("Pipeline step %s is finished. Skip this step", step));
            return;
        }
        switch (step.getEngine()) {
        case INGESTION:
            startIngest(step.getEngineName(), progress.getVersion());
            break;
        case TRANSFORMATION:
            startTransform(step.getEngineName(), progress.getVersion());
            break;
        case PUBLICATION:
            startPublish(step.getEngineName(), progress.getVersion());
            break;
        default:
            throw new UnsupportedOperationException(
                    String.format("Unsupported engine type %s in orchestration pipeline", step.getEngine().name()));
        }

        Long startTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
            engineProgress = engineService.findProgressAtVersion(step.getEngineName(), progress.getVersion());
            log.info(String.format("Progress for version %s: %s", progress.getVersion(), engineProgress.toString()));
        } while (engineProgress.getStatus() != ProgressStatus.FINISHED
                && engineProgress.getStatus() != ProgressStatus.FAILED
                && (System.currentTimeMillis() - startTime) / 1000 / 60 <= step.getTimeout());
        if (engineProgress.getStatus() == ProgressStatus.FINISHED) {
            log.info(String.format("Pipeline step %s is finished", step.toString()));
            return;
        }
        if (engineProgress.getStatus() == ProgressStatus.FAILED) {
            throw new RuntimeException(String.format("Pipeline step %s is failed. Message: %s", step.toString(),
                    engineProgress.getMessage()));
        }
        throw new RuntimeException(String.format("Pipeline step %s is timed out. Current progress: %s", step.toString(),
                engineProgress.toString()));
    }

    private void startIngest(String ingestionName, String version) {
        IngestionRequest request = new IngestionRequest();
        request.setSubmitter(ORCHESTRATION);
        request.setSourceVersion(version);
        ingestProxy.ingestInternal(ingestionName, request, progress.getHdfsPod());
    }

    private void startTransform(String transformPipeline, String version) {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(transformPipeline);
        request.setSubmitter(ORCHESTRATION);
        request.setVersion(version);
        request.setKeepTemp(true);
        transformProxy.transform(request, progress.getHdfsPod());
    }

    private void startPublish(String publishName, String version) {
        PublicationRequest request = new PublicationRequest();
        request.setSubmitter(ORCHESTRATION);
        request.setSourceVersion(version);
        publishProxy.publish(publishName, request, progress.getHdfsPod());
    }

    private void failByException(Exception e) {
        if (progress != null) {
            progress = orchestrationProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                    .message(e.getMessage()).commit(true);
            log.error(String.format("Orchestration failed for progress: %s", progress.toString()), e);
        } else {

        }
    }
}
