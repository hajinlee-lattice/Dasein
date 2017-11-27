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
        Assert.assertTrue(cdlExternalSystemStr.contains(InterfaceName.SFDC_Sandbox_ID.name()));

        String allSystem = JsonUtils.serialize(CDLExternalSystem.EXTERNAL_SYSTEM);
        Assert.assertTrue(allSystem.contains(InterfaceName.SFDC_Production_ID.name()));
        Assert.assertTrue(allSystem.contains(InterfaceName.SFDC_Sandbox_ID.name()));
        Assert.assertTrue(allSystem.contains(InterfaceName.MKTO_ID.name()));
        Assert.assertTrue(allSystem.contains(InterfaceName.ELOQUA_ID.name()));

        CDLExternalSystem.CRMType crmType = CDLExternalSystem.CRMType.fromAccountInterface(InterfaceName
                .SFDC_Sandbox_ID);
        Assert.assertTrue(crmType == CDLExternalSystem.CRMType.SFDC_Sandbox);
    }
}
