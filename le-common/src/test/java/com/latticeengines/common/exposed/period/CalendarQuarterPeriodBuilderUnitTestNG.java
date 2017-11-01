package com.latticeengines.common.exposed.period;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CalendarQuarterPeriodBuilderUnitTestNG {

    private PeriodBuilder builder = PeriodFactory.getInstance(PeriodStrategy.CalendarQuarter);

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void extractModelArtifacts(String date, int period) throws Exception {
        int actualPeriod = builder.toPeriodId(date);
        Assert.assertEquals(actualPeriod, period);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { "1987-12-31", 0 }, //
                new Object[] { "1988-01-01", 1 }, //
                new Object[] { "2016-02-29", 113 }, //
                new Object[] { "2016-04-01", 114 }, //
                new Object[] { "2017-01-30", 117 }, //
        };
    }
}
