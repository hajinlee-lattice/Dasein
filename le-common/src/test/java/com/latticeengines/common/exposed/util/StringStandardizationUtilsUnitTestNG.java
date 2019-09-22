package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StringStandardizationUtilsUnitTestNG {

    // Input, Expected Output
    @DataProvider(name = "locationStringDataProvider")
    Object[][] locationStringDataProvider() {
        return new Object[][] { //
                { "U.S.A  ", "U S A" }, //
                { "The United - States", "THE UNITED STATES" }, //
                { "United States    of    America (the)", "UNITED STATES OF AMERICA THE" }, //
        };
    }

    // Input, Expected Output
    @DataProvider(name = "nameStringDataProvider")
    Object[][] nameStringDataProvider() {
        return new Object[][] { //
                // some general cases (small case -> capital case; multiple
                // connected spaces; replace & with AND; etc)
                { "\"CarMax,    Inc's-\"", "CARMAX, INC'S-" }, //
                { "   CarMax, %@# & Inc.   ", "CARMAX, AND INC." }, //
                // test blank string
                { null, null }, //
                { "  ", null }, //
                { "\t \t", null }, //
                // test removed characters
                { " ~@ ", null }, //
                { " # $ ", null }, //
                { " % A % ", "A" }, //
                { " ^ A <> A *+= ", "A A" }, //
                { "A~@#$%^*+=<>A", "AA" }, //
                // test characters replaced by space
                { " _ ", null }, //
                { " \\| \\ ", null }, //
                { " / A \t ", "A" }, //
                { "A_A\\|A\\A/A\tA?A;A:A(A)A{A}A[A]A\rA\nA", "A A A A A A A A A A A A A A A A A" }, //
        };
    }

    // Input, Expected Output
    @DataProvider(name = "commonStringDataProvider")
    Object[][] commonStringDataProvider() {
        return new Object[][] { //
                { "   abc##$%^&@   ", "ABC##$%^&@" }, //
        };
    }

    // Input, Expected Output
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

    // Input, Expected Output
    @DataProvider(name = "inputLatticeIdStringDataProvider")
    Object[][] inputLatticeIdStringDataProvider() {
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

    // Input, Expected Output
    @DataProvider(name = "systemIdStringDataProvider")
    Object[][] systemIdStringDataProvider() {
        return new Object[][] { //
                { null, null }, //
                { "", null }, //
                { "123456789", "123456789" }, //
                { "1a2b3c4d5e", "1a2b3c4d5e" }, //
                { "   AbCDEfghiJkLmnOPQRSTuvwxyz   ", "abcdefghijklmnopqrstuvwxyz" }, //
                /*-
                 * multiple spaces into single space
                 */
                { "   AbCDEfg     hiJkL     mnOPQRSTuvwxyz   ", "abcdefg hijkl mnopqrstuvwxyz" }, //
                { "   AbCDEfg\thiJkL\nmnOPQRSTuvwxyz   ", "abcdefg hijkl mnopqrstuvwxyz" }, //
        };
    }

    // Input, Expected Output
    @DataProvider(name = "outputLatticeIdStringDataProvider")
    Object[][] outputLatticeIdStringDataProvider() {
        return new Object[][] {
                {null, null},
                {"null", null},
                {"     ", null},
                {"", null},
                {"00ABC24x3yz2", null},
                {"00账24x号yz2", null},
                {"23$#^2", null},
                {"12 345    678", null},
                {"12345\r678", null},
                {"12345\n678", null},
                {"12345\r\n678", null},
                {"00000012345000000000000000", null},
                {"12345", "0000000012345"},
                {"0012345", "0000000012345"},
                {"000000000000000012345", "0000000012345"},
                {"1234500", "0000001234500"}
        };
    }

    // Input, Expected Output
    @DataProvider(name = "nullStringDataProvider")
    Object[][] nullStringDataProvider() {
        return new Object[][] {
                { null, null }, //
                { " null ", null }, //
                { " none ", null }, //
                { " NULL ", null }, //
                { "NONE", null }, //
                { "nonenull", "nonenull" },//
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

    @Test(groups = "unit", dataProvider = "inputLatticeIdStringDataProvider")
    public void testLatticeIDStringStandardizeString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.getStandardizedInputLatticeID(input);
        Assert.assertEquals(output, expectedOutput);
    }

    @Test(groups = "unit", dataProvider = "systemIdStringDataProvider")
    public void testSystemIdStringStandardizeString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.getStandardizedSystemId(input);
        Assert.assertEquals(output, expectedOutput);
    }

    @Test(groups = "unit", dataProvider = "outputLatticeIdStringDataProvider")
    public void testStandardizeLatticeId(String input, String expectedOutput) {
//        AlertService mockAlertService = Mockito.mock(AlertService.class);
//        Mockito.doNothing().when(mockAlertService.triggerCriticalEvent(Mockito.anyString(), Mockito.anyString(),
//                Mockito.anyString(), Mockito.anyIterable()));
//        StringStandardizationUtils.setAlertService(mockAlertService);
        String output = StringStandardizationUtils.getStandardizedOutputLatticeID(input);
        Assert.assertEquals(output, expectedOutput);
    }

    @Test(groups = "unit", dataProvider = "nullStringDataProvider")
    public void testCleanNullString(String input, String expectedOutput) {
        String output = StringStandardizationUtils.cleanNullString(input);
        Assert.assertEquals(output, expectedOutput);
    }
}
