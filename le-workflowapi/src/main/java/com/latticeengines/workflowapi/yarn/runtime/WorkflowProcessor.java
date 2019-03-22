package com.latticeengines.workflowapi.yarn.runtime;

import java.util.Collection;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.ApplicationContext;

import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.util.WorkflowConfigurationUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

@StepScope
public class WorkflowProcessor extends SingleContainerYarnProcessor<WorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowProcessor.class);

    @Inject
    private ApplicationContext appContext;

    @Inject
    private SoftwareLibraryService softwareLibraryService;

    @Inject
    private WorkflowService workflowService;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Inject
    private JobCacheService jobCacheService;

    public WorkflowProcessor() {
        super();
        log.info("Constructed WorkflowProcessor.");
    }

    @Override
    public String process(WorkflowConfiguration workflowConfig) {
        if (appId == null) {
            throw new LedpException(LedpCode.LEDP_28022);
        }
        log.info(String.format("Looking up workflow for application id %s", appId));

        if (workflowConfig.getWorkflowName() == null) {
            throw new LedpException(LedpCode.LEDP_28011, new String[] { workflowConfig.toString() });
        }
        log.info(String.format("Running WorkflowProcessor with workflowName:%s and config:%s",
                workflowConfig.getWorkflowName(), workflowConfig.toString()));
        Collection<String> swpkgNames = workflowConfig.getSwpkgNames();
        if (CollectionUtils.isEmpty(swpkgNames)) {
            log.info("Enriching application context with all sw packages available.");
            appContext = softwareLibraryService.loadSoftwarePackages(SoftwareLibrary.Module.workflowapi.name(),
                    appContext);
        } else {
            log.info("Enriching application context with sw package " + swpkgNames);
            appContext = softwareLibraryService.loadSoftwarePackages(SoftwareLibrary.Module.workflowapi.name(),
                    swpkgNames, appContext);
        }

        WorkflowExecutionId workflowId;
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(appId.toString());
        if (workflowJob == null) {
            throw new RuntimeException(String.format("No workflow job found with application id %s", appId));
        }

        try {
            workflowService.prepareToSleepForCompletion();
            if (workflowConfig.isRestart()) {
                @SuppressWarnings("unchecked")
                AbstractWorkflow<? extends WorkflowConfiguration> workflow = appContext
                        .getBean(workflowConfig.getWorkflowName(), AbstractWorkflow.class);
                Class<? extends WorkflowConfiguration> workflowConfigClass = workflow.getWorkflowConfigurationType();
                WorkflowConfiguration restartWorkflowConfig = WorkflowConfigurationUtils
                        .getDefaultWorkflowConfiguration(workflowConfigClass);
                restartWorkflowConfig.setWorkflowName(workflowConfig.getWorkflowName());
                restartWorkflowConfig.setWorkflowIdToRestart(workflowConfig.getWorkflowIdToRestart());
                workflowService.registerJob(restartWorkflowConfig, appContext);
                workflowId = workflowService.restart(restartWorkflowConfig.getWorkflowIdToRestart(), workflowJob);
            } else {
                workflowService.registerJob(workflowConfig, appContext);
                workflowId = workflowService.start(workflowConfig, workflowJob);
            }
            workflowService.sleepForCompletion(workflowId);
        } catch (Exception exc) {
            workflowJob.setStatus(JobStatus.FAILED.name());
            workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            if (workflowJob.getWorkflowId() != null) {
                jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
            }

            ErrorDetails details;
            if (exc instanceof LedpException) {
                LedpException casted = (LedpException) exc;
                details = casted.getErrorDetails();
            } else {
                details = new ErrorDetails(LedpCode.LEDP_00002, exc.getMessage(), ExceptionUtils.getStackTrace(exc));
            }
            workflowJob.setErrorDetails(details);
            workflowJobEntityMgr.updateErrorDetails(workflowJob);

            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            jobUpdate.setLastUpdateTime(System.currentTimeMillis());
            workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate);
            throw new RuntimeException(exc);
        }
        return null;
    }
}
