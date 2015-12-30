package com.latticeengines.domain.exposed.dataflow;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.CreateScoreTableParameters;

public class DataFlowConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        DataFlowConfiguration payload = new DataFlowConfiguration();
        payload.setName("Payload1");
        payload.setCustomerSpace(CustomerSpace.parse("CONTRACT1.TENANT1.Production"));
        payload.setDataFlowBeanName("dataFlowBean");
        CreateScoreTableParameters dataFlowParams = new CreateScoreTableParameters("ABC", "DEF", "GHI");
        payload.setDataFlowParameters(dataFlowParams);
        
        String serializedStr = JsonUtils.serialize(payload);
        
        DataFlowConfiguration deserializedPayload = JsonUtils.deserialize(serializedStr, DataFlowConfiguration.class);
        
        assertEquals(deserializedPayload.getName(), payload.getName());
        assertEquals(deserializedPayload.getCustomerSpace().getContractId(), payload.getCustomerSpace().getContractId());
        assertEquals(deserializedPayload.getCustomerSpace().getTenantId(), payload.getCustomerSpace().getTenantId());
        assertEquals(deserializedPayload.getCustomerSpace().getSpaceId(), payload.getCustomerSpace().getSpaceId());
        assertEquals(deserializedPayload.getDataFlowBeanName(), payload.getDataFlowBeanName());
    }
}
