package com.latticeengines.workflowapi.flows;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflow;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflowConfiguration;

public class ImportMatchAndModelWorkflowInContainerDeploymentTestNG extends ImportMatchAndModelWorkflowTestNGBase {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ImportMatchAndModelWorkflowDeploymentTestNG.class);

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization/csvfiles";

    @Autowired
    private ImportMatchAndModelWorkflow importMatchAndModelWorkflow;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForImportWorkflow();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflowAccount() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Account.csv", SchemaInterpretation.LP3SalesforceAccountCSV);
        run(sourceFile);
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflowLead() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Lead.csv", SchemaInterpretation.LP3SalesforceLeadCSV);
        run(sourceFile);
    }

    private void run(SourceFile sourceFile) throws Exception {
        ImportMatchAndModelWorkflowConfiguration workflowConfig = generateWorkflowConfig(sourceFile);

        workflowConfig.setContainerConfiguration(importMatchAndModelWorkflow.name(), DEMO_CUSTOMERSPACE,
                "ImportMatchAndModelWorkflow_submitWorkflow");

        submitWorkflowAndAssertSuccessfulCompletion(workflowConfig);
    }
}
