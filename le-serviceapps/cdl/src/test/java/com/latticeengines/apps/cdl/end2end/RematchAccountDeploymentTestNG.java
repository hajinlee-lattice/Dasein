package com.latticeengines.apps.cdl.end2end;


import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class RematchAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    // no need to add this to pipeline
    // just manually verify rematch is working
    @Test(groups = "manual")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);

        // importData();

        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setFullRematch(true);
        processAnalyze(request);
    }

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Account, 3, "Account");
        Thread.sleep(2000);
    }

}
