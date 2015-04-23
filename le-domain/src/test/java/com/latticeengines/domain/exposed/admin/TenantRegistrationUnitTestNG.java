package com.latticeengines.domain.exposed.admin;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class TenantRegistrationUnitTestNG {

    @Test(groups = "unit")
    public void testTenantRegistrationUnitTestNG() throws IOException {
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "");
        String json = "{\"CustomerSpaceInfo\":" + JsonUtils.serialize(spaceInfo) + "}";
        ObjectMapper mapper = new ObjectMapper();
        TenantRegistration tenantRegistration = mapper.readValue(json, TenantRegistration.class);
        Assert.assertEquals(
                mapper.valueToTree(tenantRegistration.getSpaceInfo()),
                mapper.valueToTree(spaceInfo)
        );
    }
}
