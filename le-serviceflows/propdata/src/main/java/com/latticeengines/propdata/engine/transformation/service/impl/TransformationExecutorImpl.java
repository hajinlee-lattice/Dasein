package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.util.DateRange;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.workflow.engine.TransformationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class TransformationExecutorImpl implements TransformationExecutor {
    private HdfsPathBuilder hdfsPathBuilder;
    private String jobSubmitter;
    private TransformationWorkflowConfiguration.Builder builder = new TransformationWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;
    private TransformationService transformationService;
    private CustomerSpace customerSpace;
    private static final Log log = LogFactory.getLog(TransformationExecutor.class);
    private static final int MAX_RETRY = 3;

    public TransformationExecutorImpl(TransformationService transformationService, WorkflowProxy workflowProxy,
            HdfsPathBuilder hdfsPathBuilder) {
        this.hdfsPathBuilder = hdfsPathBuilder;
        this.transformationService = transformationService;
        this.jobSubmitter = transformationService.getClass().getSimpleName();
        this.workflowProxy = workflowProxy;
        this.customerSpace = new CustomerSpace("PropDataService", "PropDataService", "Production");
    }

    @Override
    public TransformationProgress kickOffNewProgress() {
        Integer retries = 0;
        while (retries++ < MAX_RETRY) {
            try {
                String unprocessedVersion = transformationService.findUnprocessedVersion();
                if (StringUtils.objectIsNullOrEmptyString(unprocessedVersion)) {
                    return null;
                }
                TransformationConfiguration transformationConfiguration = transformationService
                        .createTransformationConfiguration(unprocessedVersion);
                if (transformationConfiguration != null) {
                    TransformationProgress progress = transformationService
                            .startNewProgress(transformationConfiguration, jobSubmitter);
                    scheduleTransformationWorkflow(transformationConfiguration, progress);
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
    public synchronized void proceedProgress(TransformationProgress progress) {
        scheduleProceedProgressWorkflow(progress);
    }

    @Override
    public void purgeOldVersions() {
    }

    private void scheduleTransformationWorkflow(TransformationConfiguration transformationConfiguration,
            TransformationProgress progress) {
        TransformationWorkflowConfiguration configuration = builder.workflowName("propdataTransformationWorkflow")
                .payloadName("Transformation").customerSpace(customerSpace).hdfsPodId(HdfsPodContext.getHdfsPodId())
                .transformationConfiguration(transformationConfiguration)
                .rootOperationUid(progress.getRootOperationUID()).internalResourceHostPort("propdata")
                .serviceBeanName(transformationConfiguration.getServiceBeanName()).build();

        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        ApplicationId appId = ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

    private void scheduleProceedProgressWorkflow(TransformationProgress progress) {
        TransformationProgress transformationProgress = (TransformationProgress) progress;
        transformationProgress = retryJob(transformationProgress);
        switch (transformationProgress.getStatus()) {
        case NEW:
            String rootOperationUid = progress.getRootOperationUID();
            TransformationConfiguration transformationConfiguration = null;
            TransformationWorkflowConfiguration configuration = builder.workflowName("propdataTransformationWorkflow")
                    .payloadName("Transformation").customerSpace(customerSpace).hdfsPodId(HdfsPodContext.getHdfsPodId())
                    .transformationConfiguration(transformationConfiguration).rootOperationUid(rootOperationUid)
                    .internalResourceHostPort("propdata")
                    .serviceBeanName(transformationConfiguration.getServiceBeanName()).build();
            AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
            ApplicationId appId = ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
            break;
        case FINISHED:
            transformationProgress = transformationService.finish(transformationProgress);
            break;
        default:
            log.warn(String.format("Illegal starting status %s for progress %s", transformationProgress.getStatus(),
                    transformationProgress.getRootOperationUID()));
        }

        if (transformationProgress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(transformationProgress);
        } else {
            logJobSucceed(transformationProgress);
        }
    }

    private TransformationProgress retryJob(TransformationProgress progress) {
        if (ProgressStatus.FAILED.equals(progress.getStatus())) {
            log.info("Found a job to retry: " + progress);
            TransformationProgressStatus resumeStatus = TransformationProgressStatus.NEW;
            progress.setStatus(resumeStatus);
            progress.setNumRetries(progress.getNumRetries() + 1);
        }
        return progress;
    }

    private void logJobSucceed(TransformationProgress progress) {
        log.info("Transformation " + progress.getSourceName() + " finished for period "
                + new DateRange(progress.getStartDate(), progress.getEndDate()) + " RootOperationUID="
                + progress.getRootOperationUID());
    }

    private void logJobFailed(TransformationProgress progress) {
        log.error("Transformation " + progress.getSourceName() + " failed for period "
                + new DateRange(progress.getStartDate(), progress.getEndDate()) + " RootOperationUID="
                + progress.getRootOperationUID());
    }
}
