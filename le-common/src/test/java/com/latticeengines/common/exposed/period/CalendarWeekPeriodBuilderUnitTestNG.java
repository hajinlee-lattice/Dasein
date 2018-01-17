package com.latticeengines.common.exposed.period;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CalendarWeekPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void testPeriodCount(String startDate, String endDate, int period) throws Exception {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new CalendarWeekPeriodBuilder();
        } else {
            builder = new CalendarWeekPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(new Integer(actualPeriod), new Integer(period));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-02-07", 1 }, //
                new Object[] { "2016-02-05", "2016-02-10", 2 }, //
                new Object[] { "2016-02-05", "2016-09-30", 35 }, //
                new Object[] { "2017-01-01", "2017-01-02", 2 }, //
                new Object[] { "2017-01-01", "2016-12-30", 1 }, //
                new Object[] { "2017-02-15", "2017-02-15", 1 }, //
                new Object[] { "2017-02-15", "2017-02-10", 0 }, //
                new Object[] { "2017-02-15", "2017-02-05", -1 }, //

                new Object[] { null, "2000-01-02", 1 }, //
                new Object[] { null, "2000-01-03", 2 }, //
                new Object[] { null, "2000-02-01", 6 }, //
                new Object[] { null, "2016-02-29", 845 }, //
        };
    }

}
