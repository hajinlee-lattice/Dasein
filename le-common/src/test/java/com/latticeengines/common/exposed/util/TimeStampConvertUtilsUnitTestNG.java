package com.latticeengines.common.exposed.util;

import static com.latticeengines.common.exposed.util.TimeStampConvertUtils.computeTimestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeStampConvertUtilsUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(TimeStampConvertUtilsUnitTestNG.class);

    @Test(groups = { "unit", "functional" })
    public void testConvertToLong() throws Exception {
        String unsupportedDate = "13-April-17";
        Assert.expectThrows(IllegalArgumentException.class, () -> TimeStampConvertUtils.convertToLong(unsupportedDate));

        String str = "4/13/2016";
        long expectedTimestamp = computeTimestamp(str, false, "M/d/yyyy", "");
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(str), expectedTimestamp);
        // Compare with result for two digit year.  From https://solutions.lattice-engines.com/browse/PLS-11080.
        String str2 = "4/13/16";
        expectedTimestamp = computeTimestamp(str2, false, "M/d/yy", "");
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(str2), expectedTimestamp);
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(str), TimeStampConvertUtils.convertToLong(str2));

        String localeDate = "13-Feb-17";
        String normalDate = "2/13/17";
        long expected = TimeStampConvertUtils.convertToLong(normalDate);
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(localeDate), expected);

        // Test backup Natty Date Parser on date string that includes time.
        str = "2/22/2017 1:01:00 AM";
        expectedTimestamp = computeTimestamp(str, true, "M/d/yyyy h:m:s a", "");
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(str), expectedTimestamp);

        str = "2017-7-27";
        expectedTimestamp = computeTimestamp(str, false, "yyyy-M-d", "");
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(str), expectedTimestamp);

        // Test backup Natty Date Parser on date string that includes time.
        str = "2017/7/27 16:57:39";
        expectedTimestamp = computeTimestamp(str, true, "yyyy/M/d H:m:s", "");
        Assert.assertEquals(TimeStampConvertUtils.convertToLong(str), expectedTimestamp);

        // TODO(jwinter): Empty input string not handled.
        //str = "";
        //expectedTimestamp = 0;
        //Assert.assertEquals(TimeStampConvertUtils.convertToLong(str), expectedTimestamp);
    }

    @Test(groups = { "unit", "functional" })
    public void testConvertToLongWithDateTimeFormatWithENLocale() throws Exception {
        long actualTime = TimeStampConvertUtils.convertToLong("01-Feb-2018",
                "DD-MMM-YYYY", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);
        actualTime = TimeStampConvertUtils.convertToLong("01/Feb/2018",
                "DD/MMM/YYYY", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);

        actualTime = TimeStampConvertUtils.convertToLong("Feb.01.2018",
                "MMM.DD.YYYY", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);

        actualTime = TimeStampConvertUtils.convertToLong("2018/Feb/01",
                "YYYY/MMM/DD", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);

        actualTime = TimeStampConvertUtils.convertToLong("Apr-3-2018 01-23-45 Pm",
                "MMM-DD-YYYY", "00-00-00 12H", "");
        Assert.assertEquals(actualTime, 1522761825000L);
    }


    @Test(groups = { "unit", "functional" })
    public void testConvertToLongWithDateTimeFormatStringAndTimezone() throws Exception {

        // Test Case 1: Simple date with format MM/DD/YYYY, default timezone (UTC).
        long actualTime = TimeStampConvertUtils.convertToLong("02/01/2018",
                "MM/DD/YYYY", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);

        // Test Case 2: Simple date with format DD-MM-YYYY, default timezone (UTC).
        actualTime = TimeStampConvertUtils.convertToLong("02-01-2018",
                "DD-MM-YYYY", "", "");
        Assert.assertEquals(actualTime, 1514851200000L);

        // Test Case 3: Simple date with format YYYY.MM.DD, America/Los_Angeles timezone.
        actualTime = TimeStampConvertUtils.convertToLong("2017.03.04",
                "YYYY.MM.DD", "", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1488614400000L);

        // Test Case 4: Date only, with two digit year, format DD-MM-YY, default timezone (UTC).  Test post 2000 date
        // with two digits.
        actualTime = TimeStampConvertUtils.convertToLong("05-06-07",
                "DD-MM-YY", "", "");
        Assert.assertEquals(actualTime, 1181001600000L);

        // Test Case 5: Date only, with two digit year, format DD/MM/YY, default timezone (UTC).  Test 99 which for now
        // maps to 2099.
        actualTime = TimeStampConvertUtils.convertToLong("05/06/99",
                "DD/MM/YY", "", "");
        Assert.assertEquals(actualTime, 4084300800000L);

        // Test Case 6: Simple date with single digit day and month, Europe/Paris timezone (UTC+2).  Note Paris is in
        // Central European Time and in day light savings at this point and therefore ahead of UTC by 2 hours.
        actualTime = TimeStampConvertUtils.convertToLong("5.6.2018",
                "DD.MM.YYYY", "", "Europe/Paris");
        Assert.assertEquals(actualTime, 1528149600000L);

        // Test Case 7: Date/time with format MM.DD.YY 00:00:00 12H, in timezone PST.
        actualTime = TimeStampConvertUtils.convertToLong("11.02.18 11:11:11 AM",
                "MM.DD.YY", "00:00:00 12H", "PST");
        Assert.assertEquals(actualTime, 1541182271000L);

        // Test Case 8: Date/time with format DD/MM/YYYY 00 00 00 24H, in timezone America/New_York.
        actualTime = TimeStampConvertUtils.convertToLong("08/09/2018 07 07 07",
                "DD/MM/YYYY", "00 00 00 24H", "America/New_York");
                Assert.assertEquals(actualTime, 1536404827000L);

        // Test Case 9: Date/time with single digit month and day, in format MM-DD-YYYY 00-00-00 12H, in default
        // timezone (UTC).
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018 01-23-45 PM",
                "MM-DD-YYYY", "00-00-00 12H", "");
        Assert.assertEquals(actualTime, 1522761825000L);

        // Test Case 10: Date/time with single digit month and day, in format MM-DD-YYYY 00:00:00 12H, in default
        // timezone (UTC), testing lower case PM.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018 01:23:45 pm",
                "MM-DD-YYYY", "00:00:00 12H", "");
        Assert.assertEquals(actualTime, 1522761825000L);

        // Test Case 11: Date/time with single digit hour, in format YYYY/MM/DD 00 00 00, in timezone GMT-3.
        actualTime = TimeStampConvertUtils.convertToLong("2018/12/11 4 56 12",
                "YYYY/MM/DD",  "00 00 00 24H", "GMT-3");
        Assert.assertEquals(actualTime, 1544514972000L);

        // Test Case 12: Date/time with single digit month, day, hour, minute, and second, in format
        // DD/MM/YY 00-00-00 24H, in timezone GMT.
        actualTime = TimeStampConvertUtils.convertToLong("2/2/22 2-2-2",
                "DD/MM/YY",  "00-00-00 24H", "GMT");
        Assert.assertEquals(actualTime, 1643767322000L);

        // Test Case 13: Date/time with single digit month and day, in format MM-DD-YYYY 00:00:00 12H, with lowercase
        // AM, where an invalid timezone was provided and the UTC default should be used.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018 01:23:45 am",
                "MM-DD-YYYY",  "00:00:00 12H", "XXX");
        Assert.assertEquals(actualTime, 1522718625000L);

        // Test Case 14: Date in format DD/MM/YYYY with timezone.  Testing timezone correlation between different
        // formats.
        // Case 14a: EDT is not supported and equals GMT.
        actualTime = TimeStampConvertUtils.convertToLong("27/7/2017",
                "DD/MM/YYYY", "", "EDT");
        long expectedTime = TimeStampConvertUtils.computeTimestamp("27/7/2017", false,
                "d/M/yyyy", "GMT");
        Assert.assertEquals(actualTime, expectedTime);
        // Case 14b: EST does not equal America/New_York in the summer.
        actualTime = TimeStampConvertUtils.convertToLong("27/7/2017",
                "DD/MM/YYYY", "", "EST");
        expectedTime = TimeStampConvertUtils.computeTimestamp("27/7/2017", false,
                "d/M/yyyy", "America/New_York");
        Assert.assertNotEquals(actualTime, expectedTime);
        // Case 14c: EST equals America/New_York in the winter.
        actualTime = TimeStampConvertUtils.convertToLong("27/1/2017",
                "DD/MM/YYYY", "", "EST");
        expectedTime = TimeStampConvertUtils.computeTimestamp("27/1/2017", false,
                "d/M/yyyy", "America/New_York");
        Assert.assertEquals(actualTime, expectedTime);

        // Test Case 15: Test case 13 again with leading and trailing whitespace in the formats strings.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018 01:23:45 am",
                "\t\tMM-DD-YYYY    ", "		\t \t00:00:00 12H  ", "XXX");
        Assert.assertEquals(actualTime, 1522718625000L);

        // TODO(jwinter): Add support to handle empty date/time string.
        // Test Case 16: Empty string with format MM/DD/YYYY, default timezone (UTC).
        //     long actualTime = TimeStampConvertUtils.convertToLong("",
        //             "MM/DD/YYYY", "");
        //     Assert.assertEquals(actualTime, 1517443200000L);
    }

    @Test(groups = { "unit", "functional" })
    public void testConvertToDate() throws Exception {
        String str = "4/13/2016";
        long value = TimeStampConvertUtils.convertToLong(str);
        Assert.assertEquals(TimeStampConvertUtils.convertToDate(value), "2016-04-13");
    }

}
