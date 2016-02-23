package com.latticeengines.domain.exposed.metadata;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class AttributeUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        Attribute attr = new Attribute();

        attr.setApprovedUsage("Model");
        attr.setCategory("Firmographics");
        attr.setDataType("Integer");
        attr.setDescription("Description 1");
        attr.setDisplayDiscretizationStrategy("XYZ");
        attr.setDisplayName("Attribute 1");
        attr.setDataSource("DerivedColumns");
        attr.setDataQuality("xyz");
        attr.setSemanticType(SemanticType.City);

        String serializedStr = JsonUtils.serialize(attr);

        Attribute deserializedAttr = JsonUtils.deserialize(serializedStr, Attribute.class);

        assertEquals(deserializedAttr.getApprovedUsage().get(0), attr.getApprovedUsage().get(0));
        assertEquals(deserializedAttr.getDataSource().get(0), attr.getDataSource().get(0));
        assertEquals(deserializedAttr.getCategory(), attr.getCategory());
        assertEquals(deserializedAttr.getDataType(), attr.getDataType());
        assertEquals(deserializedAttr.getDescription(), attr.getDescription());
        assertEquals(deserializedAttr.getDisplayDiscretizationStrategy(), attr.getDisplayDiscretizationStrategy());
        assertEquals(deserializedAttr.getDisplayName(), attr.getDisplayName());
        assertEquals(deserializedAttr.getDataQuality(), attr.getDataQuality());
        assertEquals(deserializedAttr.getSemanticType(), attr.getSemanticType());
    }
}
