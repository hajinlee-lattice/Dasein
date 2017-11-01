package com.latticeengines.common.exposed.period;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CalendarMonthPeriodBuilderUnitTestNG {

    private PeriodBuilder builder = PeriodFactory.getInstance(PeriodStrategy.CalendarMonth);

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void extractModelArtifacts(String date, int period) throws Exception {
        int actualPeriod = builder.toPeriodId(date);
        Assert.assertEquals(new Integer(actualPeriod), new Integer(period));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { "1987-12-31", 0 }, //
                new Object[] { "1988-01-01", 1 }, //
                new Object[] { "2016-02-29", 338 }, //
                new Object[] { "2016-03-30", 339 }, //
                new Object[] { "2016-09-30", 345 }, //
                new Object[] { "2017-01-30", 349 }, //
        };
    }
}
