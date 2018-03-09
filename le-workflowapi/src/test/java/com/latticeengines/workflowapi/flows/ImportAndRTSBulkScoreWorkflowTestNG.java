package com.latticeengines.workflowapi.flows;

import java.io.InputStream;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.ImportAndRTSBulkScoreWorkflow;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class ImportAndRTSBulkScoreWorkflowTestNG extends WorkflowApiFunctionalTestNGBase {

    @Inject
    private ImportAndRTSBulkScoreWorkflow importAndRTSBulkScoreWorkflow;

    @Inject
    private ImportMatchAndModelWorkflow importMatchAndModelWorkflow;

    @Test(groups = { "functional" })
    public void test() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/workflowapi/config/workflow.conf");
        ImportMatchAndModelWorkflowConfiguration workflowConfig = JsonUtils.deserialize(is,
                ImportMatchAndModelWorkflowConfiguration.class);
        Workflow workflow = importMatchAndModelWorkflow.defineWorkflow(workflowConfig);

        System.out.println(workflow.getSteps().stream().map(AbstractStep::getNamespace).collect(Collectors.toList()));
        // System.out.println(importMatchAndModelWorkflow.buildWorkflow(workflowConfig));

    }
}
