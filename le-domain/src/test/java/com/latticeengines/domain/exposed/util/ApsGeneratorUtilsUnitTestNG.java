package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ApsGeneratorUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "provider")
    public void testIsApsAttr(String name, boolean expected) {
        Assert.assertEquals(ApsGeneratorUtils.isApsAttr(name), expected);
    }

    @DataProvider(name = "provider")
    private Object[][] provider() {
        return new Object[][] { //
                { "Product_65620DD0BCF1EC1AD2F787C3725987AB_Revenue", true }, //
                { "Product_67DFF9DA01A6C6F5111C64C9EDD385EA_Span", true }, //
                { "Product_B3564AC5E5499EF0F8D5449E92E9D2B6_RevenueRollingSum6", true }, //
                { "Product_B3564AC5E5499EF0F8D5449E92E9D2B6_RevenueMomentum3", true }, //
                { "Product_567189709581288F2516BBDBB1EDFDF4_Units", true }, //
                { "Product_567189709581288F2516BBDBB1EDFDF4_Unit", false }, //
                { "Products_567189709581288F2516BBDBB1EDFDF4_Random", false }, //
                { "Product567189709581288F2516BBDBB1EDFDF4_Units", false } //
        };
    }

}
