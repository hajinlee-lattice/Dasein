package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

public class NaturalWeekPeriodBuilderUnitTestNG extends NaturalPeriodBuilderUnitTestNGBase {

    @DataProvider(name = PERIOD_ID_DATA_PROVIDER)
    protected Object[][] providePeriodIds() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-02-07", 1 }, //
                new Object[] { "2016-02-05", "2016-02-10", 1 }, //
                new Object[] { "2016-02-05", "2016-03-09", 5 }, //
                new Object[] { "2017-01-01", "2017-01-02", 0 }, //
                new Object[] { "2017-01-01", "2016-12-30", -1 }, //
                new Object[] { "2017-02-15", "2017-02-15", 0 }, //
                new Object[] { "2017-02-15", "2017-02-10", -1 }, //
                new Object[] { "2017-02-15", "2017-02-05", -1 }, //
                new Object[] { null, "2000-01-01", 0 }, //
                new Object[] { null, "2000-01-02", 1 }, //
                new Object[] { null, "2000-02-01", 5 }, //
                new Object[] { null, "2016-02-29", 844 }, //
        };
    }

    @DataProvider(name = PERIOD_RANGE_DATA_PROVIDER)
    public Object[][] providePeriodRanges() {
        return new Object[][] { //
                new Object[] { 0, 0, null, "1999-12-26", "2000-01-01" }, //
                new Object[] { 0, 1, null, "1999-12-26", "2000-01-08" }, //
                new Object[] { 1, 2, null, "2000-01-02", "2000-01-15" }, //
                new Object[] { -1, 0, null, "1999-12-19", "2000-01-01" }, //
                new Object[] { -2, -1, null, "1999-12-12", "1999-12-25" } //
        };
    }

    @Override
    protected PeriodBuilder getBuilder() {
        return new NaturalWeekPeriodBuilder();
    }

    @Override
    protected PeriodBuilder getBuilder(String startDate) {
        return new NaturalWeekPeriodBuilder(startDate);
    }

}
