package com.latticeengines.domain.exposed.camille.lifecycle;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class CustomerSpacePropertiesUnitTestNG {
    @Test(groups = "unit")
    public void testSerDe() {
        CustomerSpaceProperties props = new CustomerSpaceProperties("name", "description", "sfdcOrg1", "sandboxSfdcOrg1");
        CustomerSpaceInfo info = new CustomerSpaceInfo(props, "");
        String serializedInfo = JsonUtils.serialize(info);
        
        CustomerSpaceInfo deserializedInfo = JsonUtils.deserialize(serializedInfo, CustomerSpaceInfo.class);
        
        assertEquals(JsonUtils.serialize(deserializedInfo), serializedInfo);
    }
}
