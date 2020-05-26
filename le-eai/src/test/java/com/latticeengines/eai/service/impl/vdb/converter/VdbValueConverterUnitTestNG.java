package com.latticeengines.eai.service.impl.vdb.converter;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class VdbValueConverterUnitTestNG {

    private VdbValueConverter converter = new VdbValueConverter();

    @Test(groups = "unit", dataProvider = "valueDataProvider")
    public void testConvert(Class<?> targetType, Object value, Object expectedValue) {
        Assert.assertEquals(converter.convertTo(targetType, value), expectedValue);
    }


    @DataProvider(name = "valueDataProvider")
    Object[][] valueDataProvider() {
        return new Object[][]{
                {Double.class, "10.1", Double.parseDouble("10.1")},
                {Integer.class, "123", Integer.valueOf("123")},
                {Integer.class, "123.123", Integer.valueOf("123")},
                {Float.class, "123.123", Float.parseFloat("123.123")},
                {Long.class, "123123123123123123", Long.valueOf("123123123123123123")},
        };
    }
}
