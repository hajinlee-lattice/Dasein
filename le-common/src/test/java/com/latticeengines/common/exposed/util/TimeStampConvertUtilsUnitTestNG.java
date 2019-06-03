package com.latticeengines.common.exposed.util;

import static com.latticeengines.common.exposed.util.TimeStampConvertUtils.computeTimestamp;
import static com.latticeengines.common.exposed.util.TimeStampConvertUtils.getAvailableTimeZoneIDs;
import static com.latticeengines.common.exposed.util.TimeStampConvertUtils.getAvailableZoneIds;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRulesException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("checkstyle:FileTabCharacter")
public class TimeStampConvertUtilsUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(TimeStampConvertUtilsUnitTestNG.class);

    @Test(groups = { "unit", "functional" })
    public void testJavaTimeZoneIdsAreValid() {
        // Test that all the supported Java time zones are valid Java time zones.
        Set<String> timeZoneIds = new LinkedHashSet<>(Arrays.asList(getAvailableTimeZoneIDs()));
        for (String javaTimeZones : TimeStampConvertUtils.getSupportedJavaTimeZones()) {
            if (TimeStampConvertUtils.SYSTEM_JAVA_TIME_ZONE.equals(javaTimeZones)) {
                Assert.assertFalse(timeZoneIds.contains(javaTimeZones));
            } else {
                Assert.assertTrue(timeZoneIds.contains(javaTimeZones));
            }
        }

        // Test that when processed, the Java time zones return the correct string.  This test is needed because
        // if Java fails to convert a TimeZone to a ZoneId, it silently fails and returns "GMT" as the ZoneId.
        for (String javaTimeZones : TimeStampConvertUtils.getSupportedJavaTimeZones()) {
            if (TimeStampConvertUtils.SYSTEM_JAVA_TIME_ZONE.equals(javaTimeZones)) {
                Assert.assertEquals(TimeZone.getTimeZone(javaTimeZones).toZoneId().getId(), "GMT");
            } else {
                Assert.assertEquals(TimeZone.getTimeZone(javaTimeZones).toZoneId().getId(), javaTimeZones);
            }
            log.info("Support Java Time Zone: " + javaTimeZones + "  Zone Id: "
                    + TimeZone.getTimeZone(javaTimeZones).toZoneId());
        }
    }

    @Test(groups = { "unit", "functional" })
    public void testOriginalConvertToLong() throws Exception {
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
    public void testConvertToLongWithDateTimeFormatWithENLocale() {
        String unsupportedDate = "13-April-17";
        Assert.expectThrows(IllegalArgumentException.class, () -> TimeStampConvertUtils.convertToLong(unsupportedDate,
                "DD-MMM-YY", "", ""));

        long actualTime = TimeStampConvertUtils.convertToLong("01-Feb-2018",
                "DD-MMM-YYYY", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);
        actualTime = TimeStampConvertUtils.convertToLong("01/Feb/2018",
                "DD/MMM/YYYY", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);

        actualTime = TimeStampConvertUtils.convertToLong("Feb.01.2018",
                "MMM.DD.YYYY", "", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1517472000000L);

        actualTime = TimeStampConvertUtils.convertToLong("2018/Feb/01",
                "YYYY/MMM/DD", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);

        // Test Case 6: Date/time with format MMM-DD-YYYY 00-00-00 12H in timezone Europe/London.
        actualTime = TimeStampConvertUtils.convertToLong("Apr-3-2018 01-23-45 Pm",
                "MMM-DD-YYYY", "00-00-00 12H", "Europe/London");
        Assert.assertEquals(actualTime, 1522758225000L);

        // Test Case 7: Date/time with format YYYY-MMM-DD 00:00 12H, in timezone America/Chicago.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("1969-Dec-31 6:10 PM",
                "YYYY-MMM-DD", "00:00 12H", "America/Chicago");
        Assert.assertEquals(actualTime, 600000L);

        // Test Case 8: Date/time with format YYYY-MMM-DD 00:00 12H, in timezone America/Chicago, and month with
        // improper casing.
        // From https://solutions.lattice-engines.com/browse/DP-9768: Fix long form month formatting.
        actualTime = TimeStampConvertUtils.convertToLong("1969-dEC-31 6:10 PM",
                "YYYY-MMM-DD", "00:00 12H", "America/Chicago");
        Assert.assertEquals(actualTime, 600000L);

        // Test Case 9: Same as 6th case above, but with April in all capitals, eg. APR.
        // From https://solutions.lattice-engines.com/browse/DP-9768: Fix long form month formatting.
        actualTime = TimeStampConvertUtils.convertToLong("APR-3-2018 01-23-45 Pm",
                "MMM-DD-YYYY", "00-00-00 12H", "Europe/London");
        Assert.assertEquals(actualTime, 1522758225000L);

        // Test Case 10: Same as 5th case above, but with February in all lowercase, eg. feb.
        // From https://solutions.lattice-engines.com/browse/DP-9768: Fix long form month formatting.
        actualTime = TimeStampConvertUtils.convertToLong("2018/feb/01",
                "YYYY/MMM/DD", "", "");
        Assert.assertEquals(actualTime, 1517443200000L);
    }

    // Test successful cases of date to timestamp conversion with various date/time formats.
    @Test(groups = { "unit", "functional" })
    public void testConvertToLongWithDateTimeFormatAndTimezone() {

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

        // Test Case 6: Simple date with single digit day and month, Europe/Berlin timezone (UTC+2).  Note Paris is in
        // Central European Time and in day light savings at this point and therefore ahead of UTC by 2 hours.
        actualTime = TimeStampConvertUtils.convertToLong("5.6.2018",
                "DD.MM.YYYY", "", "Europe/Berlin");
        Assert.assertEquals(actualTime, 1528149600000L);

        // Test Case 7: Date/time with format MM.DD.YY 00:00:00 12H, in timezone America/Los_Angeles.
        actualTime = TimeStampConvertUtils.convertToLong("11.02.18 11:11:11 AM",
                "MM.DD.YY", "00:00:00 12H", "America/Los_Angeles");
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

        // Test Case 11: Date/time with single digit hour, in format YYYY/MM/DD 00 00 00, in timezone America/Sao_Paulo.
        actualTime = TimeStampConvertUtils.convertToLong("2018/12/11 4 56 12",
                "YYYY/MM/DD", "00 00 00 24H", "America/Sao_Paulo");
        Assert.assertEquals(actualTime, 1544511372000L);

        // Test Case 12: Date/time with single digit month, day, hour, minute, and second, in format
        // DD/MM/YY 00-00-00 24H, in timezone America/Los_Angeles.
        actualTime = TimeStampConvertUtils.convertToLong("2/2/22 2-2-2",
                "DD/MM/YY", "00-00-00 24H", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1643796122000L);

        // Test Case 13: Date/time with single digit month and day, in format MM-DD-YYYY 00:00:00 12H, with lowercase
        // AM, with UTC timezone provided.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018 01:23:45 am",
                "MM-DD-YYYY", "00:00:00 12H", "UTC");
        Assert.assertEquals(actualTime, 1522718625000L);

        // Test Case 14: Test case 13 again with leading and trailing whitespace in the formats strings.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018 01:23:45 am",
                " \t \tMM-DD-YYYY	    ", "			00:00:00 12H  ", "\t			UTC ");
        Assert.assertEquals(actualTime, 1522718625000L);

        // Test Case 15: Test case 13 again with arbitrary whitespace before and between date and time in value.
        actualTime = TimeStampConvertUtils.convertToLong(
                " \t\n		4-3-2018\t \t \n\n\n\n			01:23:55 am  \t\t\n		",
                "MM-DD-YYYY", "00:00:00 12H", "UTC");
        Assert.assertEquals(actualTime, 1522718635000L);

        // Test Case 16: Simple date with format MM/DD/YYYY, test start of Epoch.
        // From https://solutions.lattice-engines.com/browse/DP-9653: Testing pre-Epoch dates.
        actualTime = TimeStampConvertUtils.convertToLong("1/1/1970",
                "MM/DD/YYYY", "", "");
        Assert.assertEquals(actualTime, 0L);

        // Test Case 17: Simple date with format MM/DD/YYYY, but the start of day before Epoch in New York.
        // From https://solutions.lattice-engines.com/browse/DP-9653: Testing pre-Epoch dates.
        actualTime = TimeStampConvertUtils.convertToLong("12/31/1969",
                "MM/DD/YYYY", "", "America/New_York");
        Assert.assertEquals(actualTime, -68400000L);

        // Test Case 18: Simple date with format MM/DD/YYYY, but the start of day before Epoch in New York.
        // From https://solutions.lattice-engines.com/browse/DP-9653: Testing pre-Epoch dates.
        actualTime = TimeStampConvertUtils.convertToLong("01/01/0100",
                "MM/DD/YYYY", "", "UTC");
        Assert.assertEquals(actualTime, -59011459200000L);

        // Test Case 19: Date/time with format MM.DD.YYYY 00:00 12H, in timezone America/Los_Angeles.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("11.02.2018 11:11 AM",
                "MM.DD.YYYY", "00:00 12H", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1541182260000L);

        // Test Case 20: Date/time with format YYYY-MM-DD 00:00 24H, in timezone America/New_York.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("2019-04-04 19:10",
                "YYYY-MM-DD", "00:00 24H", "America/New_York");
        Assert.assertEquals(actualTime, 1554419400000L);

        // Test Case 21: Date/time with format DD.MM.YY 00 00 24H, in timezone Pacific/Honolulu.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("31.01.70 00 00",
                "DD.MM.YY", "00 00 24H", "Pacific/Honolulu");
        Assert.assertEquals(actualTime, 3158388000000L);

        // Test Case 22: Test case where time is in ISO 8601 format but no date/time format is given.  Conversion code
        // reverts to legacy code which runs Natty Parser to successfully parse date.
        // From https://solutions.lattice-engines.com/browse/DP-10017
        actualTime = TimeStampConvertUtils.convertToLong("2019-04-07T23:31:04Z", "", "", "");
        Assert.assertEquals(actualTime, 1554679864000L);
    }

    // Test stripping out T and Z from dates in ISO 8601 format.
    // From https://solutions.lattice-engines.com/browse/DP-10028.
    @Test(groups = { "unit", "functional" })
    public void testConvertToLongWithISO8601() {

        // Test Case 1: Date/time with format MM.DD.YY 00:00:00 12H, in timezone America/Los_Angeles.
        long actualTime = TimeStampConvertUtils.convertToLong("11.02.18T11:11:11 AMZ",
                "MM.DD.YY", "00:00:00 12H", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1541182271000L);

        // Test Case 2: Date/time with format DD/MM/YYYY 00 00 00 24H, in timezone America/New_York.
        actualTime = TimeStampConvertUtils.convertToLong("08/09/2018t07 07 07z",
                "DD/MM/YYYY", "00 00 00 24H", "America/New_York");
        Assert.assertEquals(actualTime, 1536404827000L);

        // Test Case 3: Date/time with single digit month and day, in format MM-DD-YYYY 00-00-00 12H, in default
        // timezone (UTC).
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018T01-23-45 PMz",
                "MM-DD-YYYY", "00-00-00 12H", "");
        Assert.assertEquals(actualTime, 1522761825000L);

        // Test Case 4: Date/time with single digit month and day, in format MM-DD-YYYY 00:00:00 12H, in default
        // timezone (UTC), testing lower case PM.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018t01:23:45 pmZ",
                "MM-DD-YYYY", "00:00:00 12H", "");
        Assert.assertEquals(actualTime, 1522761825000L);

        // Test Case 5: Date/time with single digit hour, in format YYYY/MM/DD 00 00 00, in timezone America/Sao_Paulo.
        actualTime = TimeStampConvertUtils.convertToLong("2018/12/11T4 56 12",
                "YYYY/MM/DD", "00 00 00 24H", "America/Sao_Paulo");
        Assert.assertEquals(actualTime, 1544511372000L);

        // Test Case 6: Date/time with single digit month, day, hour, minute, and second, in format
        // DD/MM/YY 00-00-00 24H, in timezone America/Los_Angeles.
        actualTime = TimeStampConvertUtils.convertToLong("2/2/22T2-2-2Z",
                "DD/MM/YY", "00-00-00 24H", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1643796122000L);

        // Test Case 7: Date/time with single digit month and day, in format MM-DD-YYYY 00:00:00 12H, with lowercase
        // AM, with UTC timezone provided.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018T01:23:45 amZ",
                "MM-DD-YYYY", "00:00:00 12H", "UTC");
        Assert.assertEquals(actualTime, 1522718625000L);

        // Test Case 8: Test case 7 again with leading and trailing whitespace in the formats strings.
        actualTime = TimeStampConvertUtils.convertToLong("4-3-2018t01:23:45 amz",
                " \t \tMM-DD-YYYY    ", "			00:00:00 12H  ", "\t			UTC");
        Assert.assertEquals(actualTime, 1522718625000L);

        // Test Case 9: Date/time with format MM.DD.YYYY 00:00 12H, in timezone America/Los_Angeles.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("11.02.2018T11:11 AM",
                "MM.DD.YYYY", "00:00 12H", "America/Los_Angeles");
        Assert.assertEquals(actualTime, 1541182260000L);

        // Test Case 10: Date/time with format YYYY-MM-DD 00:00 24H, in timezone America/New_York.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("2019-04-04t19:10z",
                "YYYY-MM-DD", "00:00 24H", "America/New_York");
        Assert.assertEquals(actualTime, 1554419400000L);

        // Test Case 11: Date/time with format DD.MM.YY 00 00 24H, in timezone Pacific/Honolulu.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("31.01.70t00 00",
                "DD.MM.YY", "00 00 24H", "Pacific/Honolulu");
        Assert.assertEquals(actualTime, 3158388000000L);

        // Test Case 12: Test case with standard IS0 8601 format for date and time.
        actualTime = TimeStampConvertUtils.convertToLong("2019-04-07T23:31:04Z",
                "YYYY-MM-DD", "00:00:00 24H", "UTC");
        Assert.assertEquals(actualTime, 1554679864000L);

        // Test Case 13: Test that any random characters after the date/time format will be ignored.
        actualTime = TimeStampConvertUtils.convertToLong("2019-04-07T23:31:04Zabcdefghijklmnopqrstuvwzyz123",
                "YYYY-MM-DD", "00:00:00 24H", "UTC");
        Assert.assertEquals(actualTime, 1554679864000L);

        // Test Case 14: Test that any random characters even in case without a trailing "Z" will be ignored.  In
        // this case date/time has trailing timezone with only hours listed (-01)
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("2019-04-04t19:10-01",
                "YYYY-MM-DD", "00:00 24H", "America/New_York");
        Assert.assertEquals(actualTime, 1554419400000L);

        // Test Case 15: Date/time with format MMM-DD-YYYY 00-00-00 12H in timezone Europe/London.
        actualTime = TimeStampConvertUtils.convertToLong("Apr-3-2018T01-23-45 PmZ",
                "MMM-DD-YYYY", "00-00-00 12H", "Europe/London");
        Assert.assertEquals(actualTime, 1522758225000L);

        // Test Case 16: Date/time with format YYYY-MMM-DD 00-00 12H, in timezone America/Chicago.
        // From https://solutions.lattice-engines.com/browse/DP-9733: Testing time formats without seconds.
        actualTime = TimeStampConvertUtils.convertToLong("1969-Dec-31t6-10 PMzZzZzZz",
                "YYYY-MMM-DD", "00-00 12H", "America/Chicago");
        Assert.assertEquals(actualTime, 600000L);

        // Test Case 17: Date/time with format YYYY-MMM-DD 00-00 12H, in timezone America/Chicago, and month with
        // improper casing.
        // From https://solutions.lattice-engines.com/browse/DP-9768: Fix long form month formatting.
        actualTime = TimeStampConvertUtils.convertToLong("1969-dEC-31T6-10 PM",
                "YYYY-MMM-DD", "00-00 12H", "America/Chicago");
        Assert.assertEquals(actualTime, 600000L);

        // Test Case 18: Same as 6th case above, but with April in all capitals, eg. APR.  Date/time has trailing
        // timezone with hours and minutes (+10:00).
        // From https://solutions.lattice-engines.com/browse/DP-9768: Fix long form month formatting.
        actualTime = TimeStampConvertUtils.convertToLong("APR-3-2018t01-23-45 Pm+10:00",
                "MMM-DD-YYYY", "00-00-00 12H", "Europe/London");
        Assert.assertEquals(actualTime, 1522758225000L);

    }

    // Test error cases for date to timestamp conversion with date/time formats.
    @Test(groups = { "unit", "functional" })
    public void testConvertToLongWithFormatStringErrorCases() {
        boolean exceptionFound;

        // PLS-12095: Date Attribute : Column without date could still upload successfully.
        // Test Case 1: Date/time with date format MM/DD/YY and time format 00:00:00 12H where data/time string
        // provided only has time.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("01:15:16 PM", "MM/DD/YY", "00:00:00 12H", "UTC");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date/time value (01:15:16 PM) could not be parsed by format string: " +
                                    "MM/DD/YY 00:00:00 12H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of time without date in value.");

        // Test Case 2: Fail when date format of value does not match provided format for date/time.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("11/11/11 11 11 11", "DD.MMM.YY", "00 00 00 24H",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date/time value (11/11/11 11 11 11) could not be parsed by format string: " +
                                    "DD.MMM.YY 00 00 00 24H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of date/time not matching date format.");

        // Test Case 3: Fail when date format of value does not match provided format for date only.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("11/11/11", "YYYY-MM-DD", null, null);
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date value (11/11/11) could not be parsed by format string: YYYY-MM-DD"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of date only not matching date format.");

        // Test Case 4: Fail when date format is not a valid format for date/time.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("11/11/11 11 11 11", "DD.MMMM.YY", "00 00 00 24H",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "User provided data format is not supported: DD.MMMM.YY"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of unsupported date format.");

        // Test Case 5: Fail when time format of value does not match provided format.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("11/11/11 11 11 11", "MM/DD/YY", "00-00-00 12H",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date/time value (11/11/11 11 11 11) could not be parsed by format string: " +
                                    "MM/DD/YY 00-00-00 12H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of date/time not matching time format.");

        // Test Case 6: Fail when time format is not a valid format.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("11/11/11 11 11 11", "MM/DD/YY", "00/00/00 12H",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "User provided time format is not supported: 00/00/00 12H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of unsupported time format.");

        // Test Case 7: Fail on empty string with format MM/DD/YYYY, default timezone (UTC).
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("", "MM/DD/YYYY", "00:00:00 24H", "UTC");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date/time value () could not be parsed by format string: MM/DD/YYYY 00:00:00 24H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of empty date/time value.");

        // Test Case 8: Fall back to user only date format when date and time format are provided but value only has
        // date.
        exceptionFound = false;
        long actualTime = 0;
        try {
            actualTime = TimeStampConvertUtils.convertToLong("11/11/11", "MM/DD/YY", "00-00-00 12H",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date/time value (11/11/11) could not be parsed by format string: MM/DD/YY 00-00-00 12H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertFalse(exceptionFound, "Should not fail on case of date only provided with date and time format.");
        Assert.assertEquals(actualTime, 1320940800000L);


        // TODO(jwinter): Decide if we want to handle Case 9 by ignoring a badly formatted time component and parsing
        //     only the date component.  If the user provided a time format and the value doesn't match but we ignore
        //     it, the user will not even be informed about their incorrectly formatted data, so this is approach has
        //     trade-offs.
        /*
        // Test Case 9: Use heuristic to parse out date component when date and time format are provided but time value
        // can't be parsed.
        exceptionFound = false;
        actualTime = 0;
        try {
            actualTime = TimeStampConvertUtils.convertToLong("11/11/11 11:11:11 AM", "MM/DD/YY", "00-00-00 12H",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date/time value (11/11/11) could not be parsed by format string: MM/DD/YY 00-00-00 12H"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertFalse(exceptionFound,
                "Should not fail on case of parsable date with unparsable time provided with date and time format.");
        Assert.assertEquals(actualTime, 1320940800000L);
        */

        // Test Case 10: Use heuristic to parse out date component when only date format is provided but value has
        // date and time.
        exceptionFound = false;
        actualTime = 0;
        try {
            actualTime = TimeStampConvertUtils.convertToLong("11/11/11 11:11:11 AM", "MM/DD/YY", "",
                    "Asia/Shanghai");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date value (11/11/11 11:11:11) could not be parsed by format string: MM/DD/YY"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertFalse(exceptionFound, "Should not fail on case of data and time with only date format.");
        Assert.assertEquals(actualTime, 1320940800000L);

        // Test Case 11: Fail when user provided time zone is not a valid time zone.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("4-3-2018 01:23:45 am", "MM-DD-YYYY",  "00:00:00 12H",
                    "XXX");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "User provided time zone is not supported: XXX"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of unsupported time zone.");

        // Test Case 12: Test September as alphabetical short form with four letters.
        exceptionFound = false;
        try {
            TimeStampConvertUtils.convertToLong("2018/sept/01", "YYYY/MMM/DD", "", "");
        } catch (Exception e) {
            exceptionFound = true;
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Date value (2018/Sept/01) could not be parsed by format string: YYYY/MMM/DD"),
                    "Wrong error message: " + e.getMessage());
        }
        Assert.assertTrue(exceptionFound, "Did not fail on case of four character month.");
    }

    @Test(groups = { "unit", "functional" })
    public void testConvertToDate() {
        String str = "4/13/2016";
        long value = TimeStampConvertUtils.convertToLong(str);
        Assert.assertEquals(TimeStampConvertUtils.convertToDate(value), "2016-04-13");
    }

    @Test(groups = "manual")
    public void testDisplayTimeZonesAndZoneIds() {
        TimeZone utc12 = TimeZone.getTimeZone("UTC-12");
        log.info("Time zone (getID) for UTC-12 is: " + utc12.getID());
        log.info("To Zone Id for UTC-12 is: " + utc12.toZoneId().getId());
        log.info("Zone Id (of) for UTC-12 is: " + ZoneId.of("UTC-12"));


        TimeZone pst = TimeZone.getTimeZone("PST");
        log.info("Time zone (getID) for PST is: " + pst.getID());
        log.info("To Zone Id for PST is: " + pst.toZoneId().getId());
        // Below doesn't work.
        //log.info("Zone Id (of) for PST is: " + ZoneId.of("PST"));

        TimeZone est = TimeZone.getTimeZone("EST");
        log.info("Time zone (getID) for EST is: " + est.getID());
        log.info("To Zone Id for EST is: " + est.toZoneId().getId());
        // Below doesn't work.
        //log.info("Zone Id (of) for EST is: " + ZoneId.of("EST"));


        log.info("Time Zones are:");
        for (String timeZoneStr : getAvailableTimeZoneIDs()) {
            //log.info("   " + timeZoneStr);
            Instant instant = Instant.now();
            TimeZone timeZone = TimeZone.getTimeZone(timeZoneStr);
            log.info("TimeZone is: " + timeZone.getID()
                    + " Display Name: " + timeZone.getDisplayName()
                    + " toZoneId:" + timeZone.toZoneId().getId());
            try {
                ZoneId zoneId = ZoneId.of(timeZoneStr);
                log.info("ZoneId is: " + zoneId.getId() + " toString: " + zoneId.toString());
                ZoneOffset zoneOffset = zoneId.getRules().getOffset(instant);
                log.info("   Time Zones: " + timeZoneStr + "  ZoneOffset: " + zoneOffset.toString());
                Assert.assertEquals(timeZone.getID(), timeZone.toZoneId().getId());
            } catch (ZoneRulesException e) {
                log.warn("Cannot convert [" + timeZoneStr + "] to zone Id.");
                log.warn("Exception was: " + e.getMessage());
            }
        }

        log.info("\nZone Ids are:");
        for (String zoneIdStr : getAvailableZoneIds()) {
            log.info("   " + zoneIdStr);
            Instant instant = Instant.now();
            ZoneId zoneId = ZoneId.of(zoneIdStr);
            ZoneOffset zoneOffset = zoneId.getRules().getOffset(instant);
            log.info("   ZoneId: " + zoneIdStr + "  ZoneOffset: " + zoneOffset.toString());

        }
    }

}
