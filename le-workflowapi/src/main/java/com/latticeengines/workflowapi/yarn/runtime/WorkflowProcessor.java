package com.latticeengines.workflowapi.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.domain.exposed.workflow.YarnAppWorkflowId;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.entitymgr.YarnAppWorkflowIdEntityMgr;

@StepScope
public class WorkflowProcessor extends SingleContainerYarnProcessor<WorkflowConfiguration> {

    private static final Log log = LogFactory.getLog(WorkflowProcessor.class);

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private YarnAppWorkflowIdEntityMgr yarnAppWorkflowIdEntityMgr;

    public WorkflowProcessor() {
        super();
        log.info("Construct WorkflowProcessor");
    }

    @Override
    public String process(WorkflowConfiguration workflowConfig) throws Exception {
        log.info("Running WorkflowProcessor with config:" + workflowConfig.toString());
        appContext = loadSoftwarePackages("workflowapi", softwareLibraryService, appContext);

        WorkflowExecutionId workflowId = workflowService.start(workflowConfig.getWorkflowName(), workflowConfig);
        YarnAppWorkflowId yarnAppWorkflowId = new YarnAppWorkflowId(appId, workflowId);
        yarnAppWorkflowIdEntityMgr.create(yarnAppWorkflowId);

        WorkflowStatus workflowStatus = workflowService.waitForCompletion(workflowId);
        log.info(String.format("Completed workflow - workflowId:%s status:%s appId:%s startTime:%s endTime:%s",
                String.valueOf(workflowId.getId()), workflowStatus.getStatus(), appId.toString(),
                workflowStatus.getStartTime(), workflowStatus.getEndTime()));

        return null;
    }
}
