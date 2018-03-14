package com.latticeengines.workflowapi.flows;

import java.io.InputStream;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.CustomEventModelingWorkflow;
import com.latticeengines.cdl.workflow.ProcessAnalyzeWorkflow;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class ImportMatchAndModelWorkflowTestNG extends WorkflowApiFunctionalTestNGBase {

    @Inject
    private ImportMatchAndModelWorkflow importMatchAndModelWorkflow;

    @Inject
    private ProcessAnalyzeWorkflow processAnalyzeWorkflow;

    @Inject
    private CustomEventModelingWorkflow customEventModelingWorkflow;

    @Test(groups = { "functional" })
    public void test() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/workflowapi/config/workflow.conf");
        CustomEventModelingWorkflowConfiguration workflowConfig = JsonUtils.deserialize(is,
                CustomEventModelingWorkflowConfiguration.class);
        Workflow workflow = customEventModelingWorkflow.defineWorkflow(workflowConfig);
        System.out.println(workflow.getSteps());
        System.out
                .println(workflow.getSteps().stream().map(AbstractStep::getConfiguration).collect(Collectors.toList()));

    }
}
