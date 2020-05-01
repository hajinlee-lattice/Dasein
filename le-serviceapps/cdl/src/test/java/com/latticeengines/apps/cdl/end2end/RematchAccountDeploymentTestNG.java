package com.latticeengines.apps.cdl.end2end;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
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

        verifyResult();
    }

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Account, 3, "Account");
        Thread.sleep(2000);
    }

    private void verifyResult() {
        Table accountTable = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Assert.assertNotNull(accountTable);

        // Add few test cases to verify attribute DisplayName after rematch
        Attribute attr1 = accountTable.getAttribute("CompanyName");
        Assert.assertNotNull(attr1);
        Assert.assertEquals(StringUtils.isNotBlank(attr1.getDisplayName()), true);

        Attribute attr2 = accountTable.getAttribute("LatticeAccountId");
        Assert.assertNotNull(attr2);
        Assert.assertEquals(StringUtils.isNotBlank(attr2.getDisplayName()), true);
    }
}
