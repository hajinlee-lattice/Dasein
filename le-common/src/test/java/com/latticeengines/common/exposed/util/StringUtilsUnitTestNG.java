package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StringUtilsUnitTestNG {

    @DataProvider(name = "stringDataProvider")
    Object[][] stringDataProvider() {
        return new Object[][] { //
                { "U.S.A  ", "U S A" }, //
                { "The United - States", "THE UNITED STATES" }, //
                { "United States of America (the)", "UNITED STATES OF AMERICA THE" }, //
        };
    }

    @Test(groups = "unit", dataProvider = "stringDataProvider")
    public void testStandardizeString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.getStandardString(input);
        Assert.assertEquals(output, expectedOutput);
    }
}
