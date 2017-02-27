package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StringStandardizationUtilsUnitTestNG {

    @DataProvider(name = "locationStringDataProvider")
    Object[][] locationStringDataProvider() {
        return new Object[][] { //
                { "U.S.A  ", "U S A" }, //
                { "The United - States", "THE UNITED STATES" }, //
                { "United States    of    America (the)", "UNITED STATES OF AMERICA THE" }, //
        };
    }

    @DataProvider(name = "nameStringDataProvider")
    Object[][] nameStringDataProvider() {
        return new Object[][] { //
                { "\"CarMax,    Inc's-\"", "CARMAX, INC'S-" }, //
                { "   CarMax, %@# & Inc.   ", "CARMAX, AND INC." }, //
        };
    }

    @DataProvider(name = "commonStringDataProvider")
    Object[][] commonStringDataProvider() {
        return new Object[][] { //
                { "   abc##$%^&@   ", "ABC##$%^&@" }, //
        };
    }

    @Test(groups = "unit", dataProvider = "locationStringDataProvider")
    public void testLocationStringStandardizeString(String input, String expectedOutput) {
        String output = LocationStringStandardizationUtils.getStandardString(input);
        Assert.assertEquals(output, expectedOutput);
    }

    @Test(groups = "unit", dataProvider = "nameStringDataProvider")
    public void testNameStringStandardizeString(String input, String expectedOutput) {
        String output = NameStringStandardizationUtils.getStandardString(input);
        Assert.assertEquals(output, expectedOutput);
    }

    @Test(groups = "unit", dataProvider = "commonStringDataProvider")
    public void testCommonStringStandardizeString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.getStandardString(input);
        Assert.assertEquals(output, expectedOutput);
    }
}
