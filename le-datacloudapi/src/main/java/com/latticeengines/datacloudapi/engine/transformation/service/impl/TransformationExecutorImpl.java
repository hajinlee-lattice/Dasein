package com.latticeengines.datacloudapi.engine.transformation.service.impl;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationService;
import com.latticeengines.datacloud.workflow.engine.TransformationWorkflowConfiguration;
import com.latticeengines.datacloudapi.engine.transformation.service.TransformationExecutor;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@SuppressWarnings("rawtypes")
public class TransformationExecutorImpl implements TransformationExecutor {
    private static final Log log = LogFactory.getLog(TransformationExecutor.class);
    private static final int MAX_RETRY = 3;

    private String jobSubmitter;
    private TransformationWorkflowConfiguration.Builder builder = new TransformationWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;
    private TransformationService transformationService;
    private CustomerSpace customerSpace;

    public TransformationExecutorImpl(TransformationService transformationService, WorkflowProxy workflowProxy) {
        this.transformationService = transformationService;
        this.jobSubmitter = transformationService.getClass().getSimpleName();
        this.workflowProxy = workflowProxy;
        this.customerSpace = new CustomerSpace("PropDataService", "PropDataService", "Production");
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransformationProgress kickOffNewProgress(TransformationProgressEntityMgr transformationProgressEntityMgr,
            List<String> baseVersions, String targetVersion) {
        Integer retries = 0;
        while (retries++ < MAX_RETRY) {
            try {
                if (baseVersions == null || baseVersions.isEmpty()) {
                    baseVersions = transformationService.findUnprocessedBaseVersions();
                }
                if (CollectionUtils.isEmpty(baseVersions)) {
                    return null;
                }
                TransformationConfiguration transformationConfiguration = transformationService
                        .createTransformationConfiguration(baseVersions, targetVersion);
                if (transformationConfiguration != null) {
                    TransformationProgress progress = transformationService
                            .startNewProgress(transformationConfiguration, jobSubmitter);
                    scheduleTransformationWorkflow(transformationConfiguration, progress,
                            transformationProgressEntityMgr);
                    return progress;
                }
            } catch (Exception e) {
                log.error(e);
                throw e;
            }
        }
        throw new LedpException(LedpCode.LEDP_25015,
                new String[] { transformationService.getSource().getSourceName(), retries.toString() });
    }


    @SuppressWarnings("unchecked")
    @Override
    public TransformationProgress kickOffNewPipelineProgress(TransformationProgressEntityMgr transformationProgressEntityMgr,
           PipelineTransformationRequest request) {
        Integer retries = 0;
        while (retries++ < MAX_RETRY) {
            try {
                PipelineTransformationService service = (PipelineTransformationService) transformationService;
                TransformationConfiguration transformationConfiguration = service.createTransformationConfiguration(request);

                if (transformationConfiguration != null) {
                    TransformationProgress progress = transformationService
                            .startNewProgress(transformationConfiguration, jobSubmitter);
                    scheduleTransformationWorkflow(transformationConfiguration, progress,
                            transformationProgressEntityMgr);
                    return progress;
                }
            } catch (Exception e) {
                log.error(e);
                throw e;
            }
        }
        throw new LedpException(LedpCode.LEDP_25015,
                new String[] { transformationService.getSource().getSourceName(), retries.toString() });
    }

    @Override
    public void purgeOldVersions() {
    }

    private void scheduleTransformationWorkflow(TransformationConfiguration transformationConfiguration,
            TransformationProgress progress, TransformationProgressEntityMgr transformationProgressEntityMgr) {
        log.info("Kick off workflow for progress " + progress + " in pod " + HdfsPodContext.getHdfsPodId());

        builder = builder.workflowName("propdataTransformationWorkflow") //
                .payloadName("Transformation") //
                .customerSpace(customerSpace) //
                .hdfsPodId(HdfsPodContext.getHdfsPodId()) //
                .transformationConfiguration(transformationConfiguration) //
                .rootOperationUid(progress.getRootOperationUID()) //
                .serviceBeanName(transformationConfiguration.getServiceBeanName()) //
                .internalResourceHostPort("propdata");

        if (transformationConfiguration instanceof PipelineTransformationConfiguration) {
            PipelineTransformationConfiguration ppConf = (PipelineTransformationConfiguration) transformationConfiguration;
            builder.containerMemMB(ppConf.getContainerMemMB());
        }

        TransformationWorkflowConfiguration configuration = builder.build();

        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        progress.setYarnAppId(appSubmission.getApplicationIds().get(0));
        transformationProgressEntityMgr.updateProgress(progress);
    }
}
