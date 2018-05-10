package com.latticeengines.transform.v2_0_25.functions;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StdVisidbAlexaMonthOnlineSinceUnitTestNG {
    private Integer alexaOnlineSince;

    @Test(groups = "unit", dataProvider = "ValidCase")
    public void testValidCase(Object date) {
        if (alexaOnlineSince == null) {
            alexaOnlineSince = StdVisidbAlexaMonthssinceonline.calculateStdVisidbAlexaMonthssinceonline(date);
            Assert.assertNotNull(alexaOnlineSince);
            Assert.assertTrue(alexaOnlineSince > 0);
        } else {
            Assert.assertEquals(StdVisidbAlexaMonthssinceonline.calculateStdVisidbAlexaMonthssinceonline(date),
                    alexaOnlineSince);
        }
    }

    @Test(groups = "unit", dataProvider = "InvalidCase")
    public void testInvalidCase(Object date) {
        Assert.assertNull(StdVisidbAlexaMonthssinceonline.calculateStdVisidbAlexaMonthssinceonline(date));
    }

    @DataProvider(name = "ValidCase")
    private Object[] getValidCase() {
        return new Object[] { //
                "1514764800000", //
                1514764800000L, //
                "01/01/2018 00:00:00 AM", //
                new java.sql.Date(1514764800000L), //
                new java.util.Date(1514764800000L), //
                new java.sql.Timestamp(1514764800000L), //
        };
    }

    @DataProvider(name = "InvalidCase")
    private Object[] getInvalidCase() {
        return new Object[] { //
                null, //
                "null", //
                "2018-01-01", //
                "ABCDE",//
                "1514764800000L", //
        };
    }
}
