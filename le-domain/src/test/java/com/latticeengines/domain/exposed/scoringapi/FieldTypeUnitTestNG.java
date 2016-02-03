package com.latticeengines.domain.exposed.scoringapi;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoringapi.FieldType;

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
    }

    @Test(groups = "unit")
    public void testParseInteger() {
        FieldType type = FieldType.INTEGER;
        Object actual = FieldType.parse(type, "42");
        Assert.assertEquals(actual, new Long(42));
    }

    @Test(groups = "unit")
    public void testParseString() {
        FieldType type = FieldType.STRING;
        Object actual = FieldType.parse(type, "foo");
        Assert.assertEquals(actual, "foo");
    }

    @Test(groups = "unit")
    public void testParseTemporal() {
        FieldType type = FieldType.TEMPORAL;
        Object actual = FieldType.parse(type, "1234");
        Assert.assertEquals(actual, new Long(1234));
    }
}
