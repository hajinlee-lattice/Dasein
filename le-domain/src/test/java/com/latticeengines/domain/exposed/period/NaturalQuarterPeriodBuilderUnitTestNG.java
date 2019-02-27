package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

public class NaturalQuarterPeriodBuilderUnitTestNG extends NaturalPeriodBuilderUnitTestNGBase {

    @DataProvider(name = PERIOD_ID_DATA_PROVIDER)
    protected Object[][] providePeriodIds() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-03-30", 0 }, //
                new Object[] { "2016-02-05", "2016-09-30", 2 }, //
                new Object[] { "2016-12-31", "2017-01-30", 1 }, //
                new Object[] { "2015-12-31", "2017-01-30", 5 }, //
                new Object[] { "2017-01-01", "2016-12-30", -1 }, //
                new Object[] { "2017-04-15", "2016-08-10", -3 }, //
                new Object[] { null, "2001-08-10", 6 }, //
                new Object[] { null, "2017-12-30", 71 }, //
        };
    }

    @DataProvider(name = PERIOD_RANGE_DATA_PROVIDER)
    public Object[][] providePeriodRanges() {
        return new Object[][] { //
                new Object[] { 0, 0, null, "2000-01-01", "2000-03-31" }, //
                new Object[] { 0, 1, null, "2000-01-01", "2000-06-30" }, //
                new Object[] { 1, 2, null, "2000-04-01", "2000-09-30" }, //
                new Object[] { 4, 5, null, "2001-01-01", "2001-06-30" }, //
                new Object[] { -1, 0, null, "1999-10-01", "2000-03-31" }, //
                new Object[] { -2, -1, null, "1999-07-01", "1999-12-31" }, //
                new Object[] { -5, -4, null, "1998-10-01", "1999-03-31" }, //
                new Object[] { 1, 3, null, "2000-04-01", "2000-12-31" }, //
                new Object[] { -4, -1, null, "1999-01-01", "1999-12-31" }, //
        };
    }

    @Override
    protected PeriodBuilder getBuilder() {
        return new NaturalQuarterPeriodBuilder();
    }

    @Override
    protected PeriodBuilder getBuilder(String startDate) {
        return new NaturalQuarterPeriodBuilder(startDate);
    }


}
