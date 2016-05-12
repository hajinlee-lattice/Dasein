package com.latticeengines.domain.exposed.util;

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.scoringapi.FieldType;

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

        AttributeUtils.copyPropertiesFromAttribute(source, dest, false);
        assertEquals(dest.getDisplayName(), "foo");
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

    @Test(groups = "unit")
    public void testsetFieldMetadataFromAttribute() {
        Attribute source = new Attribute();
        source.setName("SomeAttribute");
        source.setDisplayName("foo");
        source.setNullable(true);
        source.setLogicalDataType(LogicalDataType.Reference);
        source.setDataQuality(FieldType.INTEGER.type().getSimpleName());
        source.setInterfaceName(InterfaceName.CompanyName);
        source.setApprovedUsage(ApprovedUsage.MODEL);
        source.setFundamentalType(FundamentalType.NUMERIC);
        source.setStatisticalType(StatisticalType.ORDINAL);
        source.setCategory(Category.LEAD_INFORMATION);

        FieldMetadata fm = new FieldMetadata("SomeField", FieldType.INTEGER.type());
        fm.setPropertyValue("DisplayName", "abc");

        AttributeUtils.setFieldMetadataFromAttribute(source, fm, false);
        assertEquals(fm.getPropertyValue("DisplayName"), "foo");
        assertEquals(fm.getPropertyValue("LogicalDataType"), LogicalDataType.Reference.toString());
        assertEquals(fm.getPropertyValue("DataQuality"), FieldType.INTEGER.type().getSimpleName());
        assertEquals(fm.getPropertyValue("InterfaceName"), InterfaceName.CompanyName.toString());
        assertEquals(fm.getPropertyValue("ApprovedUsage"), "[" + ApprovedUsage.MODEL.toString() + "]");
        assertEquals(fm.getPropertyValue("FundamentalType"), FundamentalType.NUMERIC.toString());
        assertEquals(fm.getPropertyValue("StatisticalType"), StatisticalType.ORDINAL.toString());
        assertEquals(fm.getPropertyValue("Category"), Category.LEAD_INFORMATION.toString());
        assertEquals(fm.getPropertyValue("Nullable"), Boolean.TRUE.toString());
        System.out.println(fm.getProperties());
    }
}
