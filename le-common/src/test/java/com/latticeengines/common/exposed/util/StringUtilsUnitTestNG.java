package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StringUtilsUnitTestNG {

    @DataProvider(name = "stringDataProvider")
    Object[][] usaDataProvider() {
        return new Object[][] { //
                { "U.S.A  ", "USA" }, //
                { "The United - States", "THE UNITED STATES" }, //
                { "United States of America (the)", "UNITED STATES OF AMERICA THE" }, //
        };
    }

    @Test(groups = "unit", dataProvider = "stringDataProvider")
    public void testStandardizeString(String input, String expectedOutput) {
        String output = StringUtils.getStandardString(input);
        Assert.assertEquals(output, expectedOutput);
    }
}
