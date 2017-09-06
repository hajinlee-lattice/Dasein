package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeStampConvertUtilsUnitTestNG {

    @Test(groups = { "unit", "functional" })
    public void testConvertToLong() throws Exception {
        String str = "4/13/2016";
        Assert.assertTrue(TimeStampConvertUtils.convertToLong(str) > 0);
        str = "2/22/2017 1:01:00 AM";
        Assert.assertTrue(TimeStampConvertUtils.convertToLong(str) > 0);
        str = "2017/7/27 16:57:39";
        Assert.assertTrue(TimeStampConvertUtils.convertToLong(str) > 0);
    }

    @Test(groups = { "unit", "functional" })
    public void testConvertToDate() throws Exception {
        String str = "4/13/2016";
        long value = TimeStampConvertUtils.convertToLong(str);
        Assert.assertEquals(TimeStampConvertUtils.convertToDate(value), "2016-04-13");
    }

}
