package com.latticeengines.domain.exposed.util;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;

public class AttributeUtilsUnitTestNG {
    @Test(groups = "unit")
    public void testMerge() {
        Attribute source = new Attribute();
        source.setDisplayName("foo");
        source.setNullable(true);
        source.setLogicalDataType(LogicalDataType.Reference);
        source.setDataQuality("Foo");
        source.setInterfaceName(InterfaceName.CompanyName);

        Attribute dest = new Attribute();
        dest.setDisplayName("bar");
        dest.setNullable(false);
        dest.setCategory("Internal");

        AttributeUtils.mergeAttributes(source, dest);
        assertEquals(dest.getDisplayName(), "bar");
        assertEquals(dest.isNullable(), Boolean.FALSE);
        assertEquals(dest.getCategory(), "Internal");
        assertEquals(dest.getLogicalDataType(), LogicalDataType.Reference);
        assertEquals(dest.getDataQuality(), "Foo");
        assertEquals(dest.getInterfaceName(), InterfaceName.CompanyName);
    }

    @Test(groups = "unit")
    public void testSetPropertiesFromStrings() {
        Attribute attribute = new Attribute();
        Map<String, String> properties = new HashMap<>();
        properties.put("InterfaceName", InterfaceName.Id.toString());
        properties.put("DisplayName", "foo");
        properties.put("LogicalDataType", LogicalDataType.Event.toString());
        AttributeUtils.setPropertiesFromStrings(attribute, properties);
        assertEquals(attribute.getInterfaceName(), InterfaceName.Id);
        assertEquals(attribute.getDisplayName(), "foo");
        assertEquals(attribute.getLogicalDataType(), LogicalDataType.Event);
    }

    @Test(groups = "unit")
    public void testSetPropertyFromString() {
        Attribute attribute = new Attribute();
        AttributeUtils.setPropertyFromString(attribute, "LogicalDataType", LogicalDataType.Event.toString());
        assertEquals(attribute.getLogicalDataType(), LogicalDataType.Event);
    }
}
