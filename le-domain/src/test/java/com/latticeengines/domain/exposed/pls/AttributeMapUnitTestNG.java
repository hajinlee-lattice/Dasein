package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class AttributeMapUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        AttributeMap map = new AttributeMap();
        
        map.put("Name", "xyz");
        map.put("Id", "123");
        
        String serializedStr = map.toString();
        System.out.println(serializedStr);
        AttributeMap deserializedMap = JsonUtils.deserialize(serializedStr, AttributeMap.class);
        
        assertEquals(deserializedMap.get("Name"), map.get("Name"));
        assertEquals(deserializedMap.get("Id"), map.get("Id"));
    }
}
