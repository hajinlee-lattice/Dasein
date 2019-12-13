package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class CDLExternalSystemUnitTestNG {

    @Test(groups = "unit")
    public void testExternalSystem() {
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add("accountId");
        crmIds.add("testId");
        crmIds.add(InterfaceName.SalesforceSandboxAccountID.name());
        cdlExternalSystem.setCRMIdList(crmIds);

        List<Pair<String, String>> idMappings = new ArrayList<>();
        idMappings.add(Pair.of("accountId", "AccountDisplayName"));
        idMappings.add(Pair.of("testId", "TestDisplayName"));
        cdlExternalSystem.setIdMapping(idMappings);

        cdlExternalSystem.setPid(1L);
        List<Pair<String, String>> outIdMappings = cdlExternalSystem.getIdMappingList();
        Assert.assertTrue(outIdMappings.size() == 2);
        Assert.assertTrue(outIdMappings.get(0).getLeft().equals("accountId"));

        String cdlExternalSystemStr = JsonUtils.serialize(cdlExternalSystem);
        Assert.assertNotNull(cdlExternalSystemStr);
        Assert.assertTrue(cdlExternalSystemStr.contains(InterfaceName.SalesforceSandboxAccountID.name()));


        Assert.assertTrue(cdlExternalSystem.getCrmIds().contains(InterfaceName.SalesforceSandboxAccountID.name()));
    }
}
