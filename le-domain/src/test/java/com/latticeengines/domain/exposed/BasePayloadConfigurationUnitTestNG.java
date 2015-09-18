package com.latticeengines.domain.exposed;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class BasePayloadConfigurationUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        BasePayloadConfiguration payload = new BasePayloadConfiguration();
        payload.setName("Payload1");
        payload.setCustomerSpace(CustomerSpace.parse("CONTRACT1.TENANT1.Production"));
        
        String serializedStr = JsonUtils.serialize(payload);
        
        BasePayloadConfiguration deserializedPayload = JsonUtils.deserialize(serializedStr, BasePayloadConfiguration.class);
        
        assertEquals(deserializedPayload.getName(), payload.getName());
        assertEquals(deserializedPayload.getCustomerSpace().getContractId(), payload.getCustomerSpace().getContractId());
        assertEquals(deserializedPayload.getCustomerSpace().getTenantId(), payload.getCustomerSpace().getTenantId());
        assertEquals(deserializedPayload.getCustomerSpace().getSpaceId(), payload.getCustomerSpace().getSpaceId());
    }
}
