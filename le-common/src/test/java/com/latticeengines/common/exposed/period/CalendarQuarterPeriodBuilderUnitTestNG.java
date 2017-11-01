package com.latticeengines.common.exposed.period;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CalendarQuarterPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void extractModelArtifacts(String startDate, String endDate, int period) throws Exception {
        PeriodBuilder builder = new CalendarQuarterPeriodBuilder(startDate);
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertTrue(actualPeriod == period);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-03-30", 0 }, //
                new Object[] { "2016-02-05", "2016-09-30", 2 }, //
                new Object[] { "2016-12-31", "2017-01-30", 1 }, //
                new Object[] { "2015-12-31", "2017-01-30", 5 }, //
        };
    }
}
