package com.latticeengines.workflowapi.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflow.exposed.service.WorkflowService;

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

    public WorkflowProcessor() {
        super();
        log.info("Construct WorkflowProcessor");
    }

    @Override
    public String process(WorkflowConfiguration workflowConfig) throws Exception {
        if (workflowConfig.getWorkflowName() == null) {
            throw new LedpException(LedpCode.LEDP_28011, new String[] { workflowConfig.toString() });
        }
        log.info(String.format("Running WorkflowProcessor with workflowName:%s and config:%s",
                workflowConfig.getWorkflowName(), workflowConfig.toString()));
        appContext = loadSoftwarePackages("workflowapi", softwareLibraryService, appContext, versionManager);

        WorkflowExecutionId workflowId;
        if (workflowConfig.isRestart()) {
            log.info("Restarting workflow " + workflowConfig.getWorkflowIdToRestart().getId());
            workflowId = workflowService.restart(workflowConfig.getWorkflowIdToRestart());
        } else {
            workflowId = workflowService.start(workflowConfig.getWorkflowName(), appId.toString(), workflowConfig);
        }

        WorkflowStatus workflowStatus = workflowService.waitForCompletion(workflowId);
        log.info(String.format("Completed workflow - workflowId:%s status:%s appId:%s startTime:%s endTime:%s",
                String.valueOf(workflowId.getId()), workflowStatus.getStatus(), appId.toString(),
                workflowStatus.getStartTime(), workflowStatus.getEndTime()));

        return null;
    }
}
