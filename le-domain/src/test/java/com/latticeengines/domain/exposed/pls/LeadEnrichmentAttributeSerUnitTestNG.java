package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;

public class LeadEnrichmentAttributeSerUnitTestNG {

    @Test(groups = "unit")
    public void testSer() {
        LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
        Map<AttributeUseCase, JsonNode> map = new HashMap<>();

        ObjectNode node = JsonUtils.createObjectNode();
        node.put("a", Boolean.parseBoolean("true"));
        node.put("b", Boolean.parseBoolean("false"));
        map.put(AttributeUseCase.CompanyProfile, node);
        attr.setAttributeFlagsMap(map);
        String jsonStr = JsonUtils.serialize(attr);
        System.out.println(jsonStr);

        LeadEnrichmentAttribute deserialized = JsonUtils.deserialize(jsonStr, LeadEnrichmentAttribute.class);
        assertEquals(deserialized.getAttributeFlagsMap().get(AttributeUseCase.CompanyProfile).get("a").asBoolean(),
                true);
        assertEquals(deserialized.getAttributeFlagsMap().get(AttributeUseCase.CompanyProfile).get("b").asBoolean(),
                false);

    }
}
