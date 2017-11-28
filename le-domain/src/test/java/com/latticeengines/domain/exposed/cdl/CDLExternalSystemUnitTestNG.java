package com.latticeengines.domain.exposed.cdl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class CDLExternalSystemUnitTestNG {

    @Test(groups = "unit")
    public void testExternalSystem() {
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        cdlExternalSystem.setCRM(CDLExternalSystem.CRMType.SFDC_Sandbox);
        cdlExternalSystem.setPid(1L);
        String cdlExternalSystemStr = JsonUtils.serialize(cdlExternalSystem);
        Assert.assertNotNull(cdlExternalSystemStr);
        Assert.assertTrue(cdlExternalSystemStr.contains(InterfaceName.SalesforceSandboxAccountID.name()));

        String allSystem = JsonUtils.serialize(CDLExternalSystem.EXTERNAL_SYSTEM);
        Assert.assertTrue(allSystem.contains(InterfaceName.SalesforceAccountID.name()));
        Assert.assertTrue(allSystem.contains(InterfaceName.SalesforceSandboxAccountID.name()));
        Assert.assertTrue(allSystem.contains(InterfaceName.MarketoAccountID.name()));
        Assert.assertTrue(allSystem.contains(InterfaceName.EloquaAccountID.name()));

        CDLExternalSystem.CRMType crmType = CDLExternalSystem.CRMType.fromAccountInterface(InterfaceName
                .SalesforceSandboxAccountID);
        Assert.assertTrue(crmType == CDLExternalSystem.CRMType.SFDC_Sandbox);
    }
}
