package com.latticeengines.workflowapi.yarn.runtime;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

@StepScope
public class WorkflowProcessor extends SingleContainerYarnProcessor<WorkflowConfiguration> {

    private static final Log log = LogFactory.getLog(WorkflowProcessor.class);

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    public WorkflowProcessor() {
        super();
        log.info("Construct WorkflowProcessor");
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
        String swlib = workflowConfig.getSwpkgName();
        if (StringUtils.isBlank(swlib)) {
            log.info("Enriching application context with all sw packages available.");
            appContext = softwareLibraryService.loadSoftwarePackages(SoftwareLibrary.Module.workflowapi.name(),
                    appContext, versionManager);
        } else {
            log.info("Enriching application context with sw package " + swlib);
            appContext = softwareLibraryService.loadSoftwarePackages(SoftwareLibrary.Module.workflowapi.name(), swlib,
                    appContext, versionManager);
        }
        workflowService.registerJob(workflowConfig.getWorkflowName(), appContext);

        WorkflowExecutionId workflowId;
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(appId.toString());
        if (workflowJob == null) {
            throw new RuntimeException(String.format("No workflow job found with application id %s", appId));
        }

        try {

            if (workflowConfig.isRestart()) {
                log.info("Restarting workflow " + workflowConfig.getWorkflowIdToRestart().getId());
                workflowId = workflowService.restart(workflowConfig.getWorkflowIdToRestart(), workflowJob);
            } else {
                workflowId = workflowService.start(workflowConfig.getWorkflowName(), workflowJob, workflowConfig);
            }

            WorkflowStatus workflowStatus = workflowService.waitForCompletion(workflowId);
            log.info(String.format("Completed workflow - workflowId:%s status:%s appId:%s startTime:%s endTime:%s",
                    String.valueOf(workflowId.getId()), workflowStatus.getStatus(), appId.toString(),
                    workflowStatus.getStartTime(), workflowStatus.getEndTime()));
        } catch (Exception e) {
            workflowJob.setStatus(FinalApplicationStatus.FAILED);
            workflowJobEntityMgr.update(workflowJob);
            throw new RuntimeException(e);
        }
        return null;
    }
}
