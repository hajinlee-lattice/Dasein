package com.latticeengines.eai.service.impl.vdb.converter;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class VdbValueConverterUnitTestNG {

    private VdbValueConverter converter = new VdbValueConverter();

    @SuppressWarnings("unchecked")
    @Test(groups = "unit", dataProvider = "valueDataProvider")
    public void testConvert(Class targetType, Object value, Object expectedValue) {
        Assert.assertEquals(converter.convertTo(targetType, value), expectedValue);
    }


    @DataProvider(name = "valueDataProvider")
    Object[][] valueDataProvider() {
        return new Object[][]{
                {Double.class, "10.1", new Double("10.1")},
                {Integer.class, "123", new Integer("123")},
                {Integer.class, "123.123", new Integer("123")},
                {Float.class, "123.123", new Float("123.123")},
                {Long.class, "123123123123123123", new Long("123123123123123123")},
        };
    }
}
