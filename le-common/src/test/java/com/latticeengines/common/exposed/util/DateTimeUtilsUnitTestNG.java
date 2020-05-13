package com.latticeengines.common.exposed.util;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DateTimeUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testDateStringConversion() {
        Date d1 = DateTimeUtils.convertToDateUTCISO8601("1970-01-01T00:00:00+0000");
        Assert.assertNotNull(d1);

        Date d2 = DateTimeUtils.convertToDateUTCISO8601("1970-01-01T00:00:00 0000");
        Assert.assertNotNull(d2);

        Assert.assertEquals(d1, d2);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testDateStringConversionBad() {
        DateTimeUtils.convertToDateUTCISO8601("bad format");
    }

    @Test(groups = "unit")
    public void testDateIdToDateStr() {
        Assert.assertEquals(DateTimeUtils.dayPeriodToDate(49278), "2018-05-30");
        System.out.println(DateTimeUtils.dateToDayPeriod("2000-01-01"));
    }

    @Test(groups = "unit")
    public void testSubtractDateString() {
        String date1 = "2000-01-10";
        String date2 = "1999-12-11";
        Assert.assertEquals(DateTimeUtils.subtractDays(date1, 30), date2);
    }

    @Test(groups = "manual")
    public void manualConversion1() {
        System.out.println(DateTimeUtils.dayPeriodToDate(85584));
    }

    @Test(groups = "manual")
    public void manualConversion2() {
        System.out.println(DateTimeUtils.dateToDayPeriod("2019-12-31"));
    }
}
