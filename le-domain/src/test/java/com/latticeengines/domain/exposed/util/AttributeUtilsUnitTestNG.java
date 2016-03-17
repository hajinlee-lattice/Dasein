package com.latticeengines.domain.exposed.util;

import static org.testng.Assert.assertEquals;

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
}
