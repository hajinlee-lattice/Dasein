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

}
