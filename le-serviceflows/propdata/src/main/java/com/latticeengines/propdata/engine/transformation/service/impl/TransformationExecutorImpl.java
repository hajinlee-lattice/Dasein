package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.workflow.engine.TransformationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

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

    @Override
    public TransformationProgress kickOffNewProgress(TransformationProgressEntityMgr transformationProgressEntityMgr) {
        Integer retries = 0;
        while (retries++ < MAX_RETRY) {
            try {
                List<String> unprocessedVersions = transformationService.findUnprocessedVersions();
                if (CollectionUtils.isEmpty(unprocessedVersions)) {
                    return null;
                }
                TransformationConfiguration transformationConfiguration = transformationService
                        .createTransformationConfiguration(unprocessedVersions);
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
            } finally {
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
        TransformationWorkflowConfiguration configuration = builder.workflowName("propdataTransformationWorkflow")
                .payloadName("Transformation").customerSpace(customerSpace).hdfsPodId(HdfsPodContext.getHdfsPodId())
                .transformationConfiguration(transformationConfiguration)
                .rootOperationUid(progress.getRootOperationUID()).internalResourceHostPort("propdata")
                .serviceBeanName(transformationConfiguration.getServiceBeanName()).build();

        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        progress.setYarnAppId(appSubmission.getApplicationIds().get(0));
        transformationProgressEntityMgr.updateProgress(progress);
    }
}
