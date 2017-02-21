package com.latticeengines.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.EntityExternalType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsWorkflowDeploymentTestNGBase;

public class CDLWorkflowDeploymentTestNGBase extends ServiceFlowsWorkflowDeploymentTestNGBase {

    protected Map<String, SourceFile> sourceFileMap = new HashMap<>();
    
    protected CustomerSpace customer = null;
    

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        customer = setupTenant();
        String resourceBase = "com/latticeengines/cdl/workflow/cdlImportWorkflow"; 
        
        sourceFileMap.put("Account", uploadFile(resourceBase, "S_Account.csv", EntityExternalType.Account));
        sourceFileMap.put("Transaction", uploadFile(resourceBase, "S_Transaction.csv", EntityExternalType.Opportunity));
        sourceFileMap.put("Product", uploadFile(resourceBase, "S_Product.csv", EntityExternalType.Product));
        
        resolveMetadata(sourceFileMap.get("Account"), SchemaInterpretation.Account, EntityExternalType.Account);
        resolveMetadata(sourceFileMap.get("Transaction"), SchemaInterpretation.TimeSeries, EntityExternalType.Opportunity);
        resolveMetadata(sourceFileMap.get("Product"), SchemaInterpretation.Category, EntityExternalType.Product);
    }
    

}
