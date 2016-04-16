package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.util.DateRange;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.workflow.engine.transform.TransformationWorkflowConfiguration;
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
        this.customerSpace = new CustomerSpace("lattice.propdata.source", "transformationEngine", "transform");
    }

    @Override
    public void kickOffNewProgress() {
        int retries = 0;
        while (retries++ < MAX_RETRY) {
            try {
                TransformationConfiguration transformationConfiguration = transformationService
                        .createTransformationConfiguration();
                if (transformationConfiguration != null) {
                    transformationService.startNewProgress(transformationConfiguration, jobSubmitter);
                    return;
                }
            } catch (Exception e) {
                log.error(e);
            } finally {
                try {
                    Thread.sleep(1800 * 1000L);
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }
        log.error(
                "Failed to find a chance to kick off a refresh of " + transformationService.getSource().getSourceName()
                        + " after " + MAX_RETRY + " retries with 0.5 hour intervals.");

        scheduleTransformationWorkflow();
    }

    @Override
    public synchronized void proceedProgress(TransformationProgress progress) {
        scheduleProceedProgressWorkflow(progress);
    }

    @Override
    public void purgeOldVersions() {
    }

    private void scheduleTransformationWorkflow() {
        TransformationConfiguration transformationConfiguration = null;
        String rootOperationUid = null;
        TransformationWorkflowConfiguration configuration = builder.workflowName("propdataTransformationWorkflow")
                .payloadName("Transformation").customerSpace(customerSpace).hdfsPodId(hdfsPathBuilder.getHdfsPodId())
                .transformationConfiguration(transformationConfiguration).rootOperationUid(rootOperationUid).build();

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
                    .payloadName("Transformation").customerSpace(customerSpace)
                    .hdfsPodId(hdfsPathBuilder.getHdfsPodId()).transformationConfiguration(transformationConfiguration)
                    .rootOperationUid(rootOperationUid).build();
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
