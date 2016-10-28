package com.latticeengines.cdl.workflow;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsWorkflowDeploymentTestNGBase;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-cdl-context.xml" })
public class CDLImportWorkflowDeploymentTestNG extends ServiceFlowsWorkflowDeploymentTestNGBase {
    
    @Autowired
    private CDLImportWorkflow cdlImportWorkflow;
    
    private Map<String, SourceFile> sourceFileMap = new HashMap<>();
    
    private CustomerSpace customer = null;
    
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        customer = setupTenant();
        String resourceBase = "com/latticeengines/cdl/workflow/cdlImportWorkflow"; 
        
        sourceFileMap.put("Account", uploadFile(resourceBase, "S_Account.csv"));
        sourceFileMap.put("Transaction", uploadFile(resourceBase, "S_Transaction.csv"));
        sourceFileMap.put("Product", uploadFile(resourceBase, "S_Product.csv"));
        
        resolveMetadata(sourceFileMap.get("Account"), SchemaInterpretation.Account);
        resolveMetadata(sourceFileMap.get("Transaction"), SchemaInterpretation.TimeSeries);
        resolveMetadata(sourceFileMap.get("Product"), SchemaInterpretation.Category);
        
        
    }
    
    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        CDLImportWorkflowConfiguration config = generateConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(cdlImportWorkflow.name(), config);

        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }
    
    private CDLImportWorkflowConfiguration generateConfiguration() {
        CDLImportWorkflowConfiguration.Builder builder = new CDLImportWorkflowConfiguration.Builder();
        return builder.microServiceHostPort(microServiceHostPort) //
                .internalResourceHostPort(plsUrl) //
                .customer(customer) //
                .accountSourceType(SourceType.FILE) //
                .accountSourceFileName(sourceFileMap.get("Account").getName()) //
                .timeSeriesSourceFileName("Transaction", sourceFileMap.get("Transaction").getName()) //
                .timeSeriesSourceType("Transaction", SourceType.FILE) //
                .categorySourceFileName("Product", sourceFileMap.get("Product").getName()) //
                .categorySourceType("Product", SourceType.FILE) //
                .build();
    }
}
