package com.latticeengines.eai.file.runtime.mapreduce;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.TimeStampConvertUtils;

public class CSVImportMapperUnitTestNG {

    private CSVImportMapper mapper = new CSVImportMapper();

    @Test(groups = "unit", dataProvider = "testNumStr")
    public void testParseStringToNumber(String numStr, Number expectedValue, Boolean shouldPass) {
        try {
            Number result = mapper.parseStringToNumber(numStr);
            if (!shouldPass) {
                // should throw a parse error
                Assert.fail("Should HAVE thrown exceptions.");
            }

            // transform result to different expected number type
            if (expectedValue instanceof Integer) {
                Assert.assertEquals(result.intValue(), expectedValue);
            } else if (expectedValue instanceof Long) {
                Assert.assertEquals(result.longValue(), expectedValue);
            } else if (expectedValue instanceof Float) {
                Assert.assertEquals(result.floatValue(), expectedValue);
            } else {
                Assert.assertEquals(result.doubleValue(), expectedValue);
            }
        } catch (Exception e) {
            if (shouldPass) {
                // should not throw errror
                Assert.fail("Should not have thrown exceptions.");
            } else {
                Assert.assertTrue(true, "Should not have thrown exceptions.");
            }
        }
    }

    /**
     * Generate the testing data for {@link CSVImportMapper#parseStringToNumber(String)}
     * @return array of test data objects, each object is another object array where
     * the first item is the number string
     * the second item is expected number (type matters)
     * the third item is a flag to specify whether the test should pass
     * E.g., { "1135.00", 1135L, true } means parseStringToNumber("1135.00").longValue() should
     * match 1135
     */
    @DataProvider(name = "testNumStr")
    public Object[][] provideTestNumberStrings() {
        return new Object[][] {
                // valid number string tests
                { " 1,135.00", 1135, true },
                { "1,233,445,212,314.00", 1233445212314L, true },
                { "123,123.01", 123123.01f, true },
                // invalid number string tests
                { "123,123.01ac", null, false },
                { "ac123,123.01", null, false },
                // scientific notation tests
                { "4.86E+11", 486000000000.0, true },
                { "4.86E-3", 0.00486, true },
                { "1e-5", 0.00001, true },
                { "12345", 12345.0, true },
                { "123.456e2", 12345.6, true },
                { "0e1", 0.0, true },
                { "0.123e1", 1.23, true }
        };
    }

    @Test(groups = "unit", dataProvider = "testDateStr")
    public void testIsEmptyString(String dateStr, boolean expectedValue) {
        Assert.assertEquals(mapper.isEmptyString(dateStr), expectedValue);
    }

    @DataProvider(name = "testDateStr")
    public Object[][] provideTestDateStrings() {
        return new Object[][]{
                {"none", true},
                {"Null", true},
                {"nA", true},
                {"N/A", true},
                {"blank", true},
                {"Empty", true},
                {"test", false}
        };
    }

    @Test(groups = "unit", dataProvider = "testTimeZoneStr")
    public void testTimeZoneCheck(String dateStr, boolean isISOValue) {
        try {
            mapper.checkTimeZoneValidity(dateStr, null);
        } catch(Exception e) {
            Assert.fail("should not have thrown Exception");
        }
        try {
            mapper.checkTimeZoneValidity(dateStr, TimeStampConvertUtils.SYSTEM_JAVA_TIME_ZONE);
        } catch(Exception e) {
            if (isISOValue) {
                Assert.fail("Should not have thrown Exception");
            } else {
                Assert.assertTrue(e instanceof IllegalArgumentException);
            }
        }
        try {
            mapper.checkTimeZoneValidity(dateStr, "UTC");
        } catch(Exception e) {
            if (isISOValue) {
                Assert.assertTrue(e instanceof  IllegalArgumentException);
            } else {
                Assert.fail("Should not have thrown exception");
            }
        }
    }

    @DataProvider(name = "testTimeZoneStr")
    public Object[][] provideTestTimeZoneStrings() {
        return new Object[][] {
                // exceptional value
                {"null", false},
                {"", false},
                {"   ", false},
                // valid date time
                {"2013/09/24", false},
                {"2013/09/23 07:24", false},
                {"2014-09-23   06:25", false},
                // T&Z value
                {"2014/09/24T07:23", true},
                {"2014/09/24T07:23Z", true},
                {"2014/09/24T07:23 PM", true},
                {"2014/09/24T05:24 PMZ", true},
                {"12.12.68T12:12:12-0900", true},
                {"2014/09/24 07:23Z", false}
        };
    }
}
