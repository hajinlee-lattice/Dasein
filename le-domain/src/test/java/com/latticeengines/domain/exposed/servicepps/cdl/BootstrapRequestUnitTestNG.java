package com.latticeengines.domain.exposed.servicepps.cdl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLBootstrapRequest;
import com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest;

public class BootstrapRequestUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        CDLBootstrapRequest cdlBootstrapRequest = new CDLBootstrapRequest();
        cdlBootstrapRequest.setTenantId("Tenant1");

        String serialized = JsonUtils.serialize(cdlBootstrapRequest);

        BootstrapRequest bootstrapRequest = JsonUtils.deserialize(serialized, BootstrapRequest.class);

        Assert.assertTrue(bootstrapRequest instanceof CDLBootstrapRequest);
        Assert.assertEquals(bootstrapRequest.getTenantId(), "Tenant1");
    }

}
