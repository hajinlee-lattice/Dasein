package com.latticeengines.workflowapi.flows;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.leadprioritization.workflow.CreateModelWorkflow;
import com.latticeengines.leadprioritization.workflow.CreateModelWorkflowConfiguration;

public class CreateModelWorkflowInContainerDeploymentTestNG extends CreateModelWorkflowTestNGBase {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(CreateModelWorkflowDeploymentTestNG.class);

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization/csvfiles";

    @Autowired
    private CreateModelWorkflow createModelWorkflow;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForImportWorkflow();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflowAccount() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Account.csv", SchemaInterpretation.SalesforceAccount);
        run(sourceFile);
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflowLead() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Lead.csv", SchemaInterpretation.SalesforceLead);
        run(sourceFile);
    }

    private void run(SourceFile sourceFile) throws Exception {
        CreateModelWorkflowConfiguration workflowConfig = generateWorkflowConfig(sourceFile);

        workflowConfig.setContainerConfiguration(createModelWorkflow.name(), DEMO_CUSTOMERSPACE,
                "CreateModelWorkflow_submitWorkflow");

        submitWorkflowAndAssertSuccessfulCompletion(workflowConfig);
    }
}
