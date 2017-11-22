package com.latticeengines.common.exposed.period;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CalendarMonthPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void extractModelArtifacts(String startDate, String endDate, int period) throws Exception {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new CalendarMonthPeriodBuilder();
        } else {
            builder = new CalendarMonthPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(new Integer(actualPeriod), new Integer(period));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-02-29", 1 }, //
                new Object[] { "2016-02-05", "2016-03-30", 2 }, //
                new Object[] { "2016-02-05", "2016-09-30", 8 }, //
                new Object[] { "2016-12-31", "2017-01-30", 2 }, //
                new Object[] { "2015-12-31", "2017-01-30", 14 }, //
                new Object[] { "2017-01-01", "2016-12-30", 0 }, //
                new Object[] { "2017-02-15", "2016-10-10", -3 }, //

                new Object[] { null, "2000-02-01", 2 }, //
                new Object[] { null, "2016-02-29", 194 }, //
        };
    }
}
