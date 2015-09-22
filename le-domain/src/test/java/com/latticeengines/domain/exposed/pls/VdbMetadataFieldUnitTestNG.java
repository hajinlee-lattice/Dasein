package com.latticeengines.domain.exposed.pls;

import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class VdbMetadataFieldUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        VdbMetadataField field = getTestField();
        String serializedStr = field.toString();
        VdbMetadataField deserializedField = JsonUtils.deserialize(serializedStr, VdbMetadataField.class);
        Assert.assertTrue(field.equals(deserializedField));
        Assert.assertEquals(field.hashCode(), deserializedField.hashCode());
    }

    @Test(groups = "unit")
    public void testEquals() {
        VdbMetadataField field1 = getTestField();
        Assert.assertTrue(field1.equals(field1));
        VdbMetadataField field2 = getTestField();
        Assert.assertTrue(field1.equals(field2));
        Assert.assertEquals(field1.hashCode(), field2.hashCode());
    }

    @Test(groups = "unit", dataProvider = "vdbMetadataFieldDataProviderEqualsArgs")
    public void testNotEquals(VdbMetadataField field) {
        VdbMetadataField fieldBase = getTestField();
        Assert.assertFalse(fieldBase.equals(field));
        Assert.assertFalse(fieldBase.hashCode() == field.hashCode());
    }

    @Test(groups = "unit")
    public void testClone() {
        VdbMetadataField field1 = getTestField();
        VdbMetadataField field2 = (VdbMetadataField)field1.clone();
        Assert.assertEquals(field1.getColumnName(), field2.getColumnName());
        Assert.assertEquals(field1.getSource(), field2.getSource());
        Assert.assertEquals(field1.getSourceToDisplay(), field2.getSourceToDisplay());
        Assert.assertEquals(field1.getDisplayName(), field2.getDisplayName());
        Assert.assertEquals(field1.getTags(), field2.getTags());
        Assert.assertEquals(field1.getCategory(), field2.getCategory());
        Assert.assertEquals(field1.getApprovedUsage(), field2.getApprovedUsage());
        Assert.assertEquals(field1.getFundamentalType(), field2.getFundamentalType());
        Assert.assertEquals(field1.getDescription(), field2.getDescription());
        Assert.assertEquals(field1.getDisplayDiscretization(), field2.getDisplayDiscretization());
        Assert.assertEquals(field1.getStatisticalType(), field2.getStatisticalType());
    }

    @DataProvider(name = "vdbMetadataFieldDataProviderNotEqualsArgs")
    public static Object[][] vdbMetadataFieldDataProviderEqualsArgs() {
        VdbMetadataField field = getTestField();
        VdbMetadataField field1 = (VdbMetadataField)field.clone();
        field1.setColumnName("A1");
        VdbMetadataField field2 = (VdbMetadataField)field.clone();
        field2.setSource("B1");
        VdbMetadataField field3 = (VdbMetadataField)field.clone();
        field3.setSourceToDisplay("C1");
        VdbMetadataField field4 = (VdbMetadataField)field.clone();
        field4.setDisplayName("D1");
        VdbMetadataField field5 = (VdbMetadataField)field.clone();
        field5.setTags("E1");
        VdbMetadataField field6 = (VdbMetadataField)field.clone();
        field6.setCategory("F1");
        VdbMetadataField field7 = (VdbMetadataField)field.clone();
        field7.setApprovedUsage("G1");
        VdbMetadataField field8 = (VdbMetadataField)field.clone();
        field8.setFundamentalType("H1");
        VdbMetadataField field9 = (VdbMetadataField)field.clone();
        field9.setDescription("I1");
        VdbMetadataField field10 = (VdbMetadataField)field.clone();
        field10.setDisplayDiscretization("J1");
        VdbMetadataField field11 = (VdbMetadataField)field.clone();
        field11.setStatisticalType("K1");

        return new Object[][] { { field1 }, //
                { field2 }, //
                { field3 }, //
                { field4 }, //
                { field5 }, //
                { field6 }, //
                { field7 }, //
                { field8 }, //
                { field9 }, //
                { field10 }, //
                { field11 } };
    }

    private static VdbMetadataField getTestField() {
        VdbMetadataField field = new VdbMetadataField();
        field.setColumnName("A");
        field.setSource("B");
        field.setSourceToDisplay("C");
        field.setDisplayName("D");
        field.setTags("E");
        field.setCategory("F");
        field.setApprovedUsage("G");
        field.setFundamentalType("H");
        field.setDescription("I");
        field.setDisplayDiscretization("J");
        field.setStatisticalType("K");

        return field;
    }

}
