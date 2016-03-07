package com.latticeengines.domain.exposed.metadata;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

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

    @Test(groups = "unit")
    public void testSetPropertyValueFromString() {
        List<String> approvedUsage = new ArrayList<>();
        approvedUsage.add(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        approvedUsage.add(ModelingMetadata.MODEL_APPROVED_USAGE);
        String string = approvedUsage.toString();
        Attribute attribute = new Attribute();
        attribute.setPropertyValueFromString("ApprovedUsage", string);
        List<String> result = attribute.getApprovedUsage();
        assertEquals(result.size(), approvedUsage.size());
        for (int i = 0; i < result.size(); ++i) {
            assertEquals(result.get(i), approvedUsage.get(i));
        }

        approvedUsage.clear();
        string = approvedUsage.toString();
        attribute.setPropertyValueFromString("ApprovedUsage", string);
        result = attribute.getApprovedUsage();
        assertEquals(result.size(), 0);
    }
}
