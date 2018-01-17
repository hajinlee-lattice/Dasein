package com.latticeengines.common.exposed.period;


import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CalendarYearPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void testPeriodCount(String startDate, String endDate, int period) throws Exception {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new CalendarYearPeriodBuilder();
        } else {
            builder = new CalendarYearPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(actualPeriod, period);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-01-01", 1 }, //
                new Object[] { "2016-02-05", "2015-12-31", 0 }, //
                new Object[] { "2016-12-31", "2017-01-30", 2 }, //
                new Object[] { "2015-01-01", "2017-12-30", 3 }, //
                new Object[] { "2017-01-01", "2016-12-30", 0 }, //
                new Object[] { "2017-04-15", "2016-08-10", 0 }, //

                new Object[] { null, "2001-08-10", 2 }, //
                new Object[] { null, "2017-12-30", 18 }, //
        };
    }

}
