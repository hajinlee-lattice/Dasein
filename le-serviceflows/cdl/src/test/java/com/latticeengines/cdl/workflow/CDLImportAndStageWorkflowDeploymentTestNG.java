package com.latticeengines.cdl.workflow;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLCreateStagingTablesWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

public class CDLImportAndStageWorkflowDeploymentTestNG extends CDLWorkflowDeploymentTestNGBase {

    @Autowired
    private CDLImportWorkflow cdlImportWorkflow;

    @Autowired
    private CDLCreateStagingTablesWorkflow cdlCreateStagingTablesWorkflow;

    private Map<String, SourceFile> sourceFileMap = new HashMap<>();

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupEnvironment();
        String resourceBase = "com/latticeengines/cdl/workflow/cdlImportWorkflow";

        sourceFileMap.put("Account", uploadFile(resourceBase, "S_Account.csv", BusinessEntity.Account));
        sourceFileMap.put("Transaction", uploadFile(resourceBase, "S_Transaction.csv", BusinessEntity.Transaction));
        sourceFileMap.put("Product", uploadFile(resourceBase, "S_Product.csv", BusinessEntity.Product));

        resolveMetadata(sourceFileMap.get("Account"), SchemaInterpretation.Account, BusinessEntity.Account);
        resolveMetadata(sourceFileMap.get("Transaction"), SchemaInterpretation.TimeSeries, BusinessEntity.Transaction);
        resolveMetadata(sourceFileMap.get("Product"), SchemaInterpretation.Category, BusinessEntity.Product);
    }

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        CDLImportWorkflowConfiguration config = generateConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(config);

        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testWorkflow" })
    public void testCreateStagingTablesWorkflow() throws Exception {
        CDLCreateStagingTablesWorkflowConfiguration config = generateCreateStagingTablesConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(config);

        System.out.println("Create staging tables workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    private CDLCreateStagingTablesWorkflowConfiguration generateCreateStagingTablesConfiguration() {
        CDLCreateStagingTablesWorkflowConfiguration.Builder builder = new CDLCreateStagingTablesWorkflowConfiguration.Builder();
        return builder.internalResourceHostPort(plsUrl) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(customer) //
                .customerDataPath(PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customer).toString()) //
                .sourceFile("Account", sourceFileMap.get("Account")) //
                .sourceFile("Transaction", sourceFileMap.get("Transaction")) //
                .sourceFile("Product", sourceFileMap.get("Product")) //
                .build();
    }

    private CDLImportWorkflowConfiguration generateConfiguration() {
        CDLImportWorkflowConfiguration.Builder builder = new CDLImportWorkflowConfiguration.Builder();
        return builder.microServiceHostPort(microServiceHostPort) //
                .internalResourceHostPort(plsUrl) //
                .customer(customer) //
                .sourceFile("Account", sourceFileMap.get("Account")) //
                .sourceFile("Transaction", sourceFileMap.get("Transaction")) //
                .sourceFile("Product", sourceFileMap.get("Product")) //
                .build();
    }
}
