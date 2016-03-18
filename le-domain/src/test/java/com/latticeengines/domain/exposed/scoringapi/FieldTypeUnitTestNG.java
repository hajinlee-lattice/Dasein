package com.latticeengines.domain.exposed.scoringapi;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldTypeUnitTestNG {
    @Test(groups = "unit")
    public void testParseBoolean() {
        FieldType type = FieldType.BOOLEAN;
        Object actual = FieldType.parse(type, "1");
        Assert.assertEquals(actual, Boolean.TRUE);
        actual = FieldType.parse(type, "true");
        Assert.assertEquals(actual, Boolean.TRUE);
        actual = FieldType.parse(type, "false");
        Assert.assertEquals(actual, Boolean.FALSE);
        actual = FieldType.parse(type, "      false         ");
        Assert.assertEquals(actual, Boolean.FALSE);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testParseInvalidBoolean() {
        FieldType type = FieldType.BOOLEAN;
        FieldType.parse(type, "fasle");
    }

    @Test(groups = "unit")
    public void testParseFloat() {
        FieldType type = FieldType.FLOAT;
        Object actual = FieldType.parse(type, "1.03");
        Double dactual = (Double) actual;
        Assert.assertTrue(Math.abs(dactual.doubleValue() - 1.03) < 1e-10);
        Assert.assertEquals(FieldType.parse(FieldType.FLOAT, "      1.03      "), new Double(1.03));
    }

    @Test(groups = "unit")
    public void testParseInteger() {
        FieldType type = FieldType.INTEGER;
        Object actual = FieldType.parse(type, "42");
        Assert.assertEquals(actual, new Long(42));
        Assert.assertEquals(FieldType.parse(FieldType.INTEGER, "      42      "), new Long(42));
    }

    @Test(groups = "unit")
    public void testParseLong() {
        FieldType type = FieldType.LONG;
        Object actual = FieldType.parse(type, "9223372036854775807");
        Assert.assertEquals(actual, Long.MAX_VALUE);
        actual = FieldType.parse(type, "      9223372036854775807       ");
        Assert.assertEquals(actual, Long.MAX_VALUE);
    }

    @Test(groups = "unit")
    public void testParseString() {
        FieldType type = FieldType.STRING;
        Object actual = FieldType.parse(type, "foo");
        Assert.assertEquals(actual, "foo");
        actual = FieldType.parse(type, "         foo   ");
        Assert.assertEquals(actual, "         foo   ");
    }

    @Test(groups = "unit", dataProvider = "allTypes")
    public void testAvroTypes(String avroType, FieldType type) {
        Assert.assertEquals(FieldType.getFromAvroType(avroType), type);
    }

    @Test(groups = "unit")
    public void testParseEmptyString() {
        Assert.assertNull(FieldType.parse(FieldType.BOOLEAN, ""));
        Assert.assertNull(FieldType.parse(FieldType.BOOLEAN, "  "));
        Assert.assertNull(FieldType.parse(FieldType.LONG, "  "));
        Assert.assertEquals(FieldType.parse(FieldType.STRING, ""), "");
        Assert.assertEquals(FieldType.parse(FieldType.STRING, "   "), "   ");
    }

    @DataProvider(name = "allTypes")
    public Object[][] allTypes() {
        return new Object[][] { //
            { "boolean", FieldType.BOOLEAN }, //
            { "int", FieldType.INTEGER }, //
            { "long", FieldType.LONG }, //
            { "float", FieldType.FLOAT }, //
            { "double", FieldType.FLOAT }, //
            { "string", FieldType.STRING } //
        };

    }
}
