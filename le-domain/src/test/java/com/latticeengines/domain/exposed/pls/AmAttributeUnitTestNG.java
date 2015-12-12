package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class AmAttributeUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        AmAttribute attr = new AmAttribute();
        
        attr.setPid(new Long(123));
        attr.setAttrKey("Region");
        attr.setAttrValue("Western");
        attr.setParentKey("Country");
        attr.setParentValue("USA");
        attr.setParentValue("Account");
        
        String serializedStr = attr.toString();
        System.out.println(serializedStr);
        AmAttribute deserializedAttr = JsonUtils.deserialize(serializedStr, AmAttribute.class);
        
        assertEquals(deserializedAttr.getAttrKey(), attr.getAttrKey());
        assertEquals(deserializedAttr.getAttrValue(), attr.getAttrValue());
        assertEquals(deserializedAttr.getParentKey(), attr.getParentKey());
        assertEquals(deserializedAttr.getParentValue(), attr.getParentValue());
        assertEquals(deserializedAttr.getSource(), attr.getSource());
    }
}
