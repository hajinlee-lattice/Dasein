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

    @DataProvider(name = "dunsStringDataProvider")
    Object[][] dunsStringDataProvider() {
        return new Object[][] { //
                { "491vls320", "000491320" }, //
                { "491vls321320", "491321320" }, //
                { "491vls3213200", null }, //
                { "123456789", "123456789" }, //
                { "123", "000000123" }, //
                { "1234567890", null }
        };
    }

    @DataProvider(name = "latticeIDStringDataProvider")
    Object[][] latticeIDStringDataProvider() {
        return new Object[][] {
                { null, null },
                { "", null },
                { "   ", null },
                { "  \t   ", null },
                { "null", null },
                { "Null", null },
                { "NULL", null },
                { " NULL", null },
                { "NULL ", null },
                { "标识符", null },
                { "1标23识456符", null },
                { "123abc456", null },
                { "123A!B@3^#$D456A", null },
                { "123A3^   $D456A", null },
                { "-12345", null },
                { "+12345", null },
                { "1 23  4\t5", null },
                { "1234567890123456789", null },
                { "-123456789012", null },
                { "+123456789012", null },
                { "      123456", "123456" },
                { "123456     ", "123456" },
                { "      123456     ", "123456" },
                { "12345", "12345" },
                { "1114567890123", "1114567890123" },
                { "00001234567890123", "1234567890123" },
                { "0001230045600", "1230045600" },
                { "000000000000001230045600", "1230045600" }
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
    
    @Test(groups = "unit", dataProvider = "dunsStringDataProvider")
    public void testDunsStringStandardizeString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.getStandardDuns(input);
        Assert.assertEquals(output, expectedOutput);
    }

    @Test(groups = "unit", dataProvider = "latticeIDStringDataProvider")
    public void testLatticeIDStringStandardizeString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.getStandardizedInputLatticeID(input);
        Assert.assertEquals(output, expectedOutput);
    }
}
