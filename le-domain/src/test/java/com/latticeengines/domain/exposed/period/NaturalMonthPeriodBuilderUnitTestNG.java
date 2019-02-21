package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

public class NaturalMonthPeriodBuilderUnitTestNG extends NaturalPeriodBuilderUnitTestNGBase {

    @DataProvider(name = PERIOD_ID_DATA_PROVIDER)
    protected Object[][] providePeriodIds() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-02-29", 0 }, //
                new Object[] { "2016-02-05", "2016-03-30", 1 }, //
                new Object[] { "2016-02-05", "2016-09-30", 7 }, //
                new Object[] { "2016-12-31", "2017-01-30", 1 }, //
                new Object[] { "2015-12-31", "2017-01-30", 13 }, //
                new Object[] { "2017-01-01", "2016-12-30", -1 }, //
                new Object[] { "2017-02-15", "2016-10-10", -4 }, //
                new Object[] { null, "2000-02-01", 1 }, //
                new Object[] { null, "2016-02-29", 193 }, //
        };
    }

    @DataProvider(name = PERIOD_RANGE_DATA_PROVIDER)
    public Object[][] providePeriodRanges() {
        return new Object[][] { //
                new Object[] { 0, 0, null, "2000-01-01", "2000-01-31" }, //
                new Object[] { 0, 1, null, "2000-01-01", "2000-02-29" }, //
                new Object[] { 1, 2, null, "2000-02-01", "2000-03-31" }, //
                new Object[] { 11, 12, null, "2000-12-01", "2001-01-31" }, //
                new Object[] { 12, 13, null, "2001-01-01", "2001-02-28" }, //
                new Object[] { -1, 0, null, "1999-12-01", "2000-01-31" }, //
                new Object[] { -2, -1, null, "1999-11-01", "1999-12-31" }, //
                new Object[] { -13, -12, null, "1998-12-01", "1999-01-31" }, //
                new Object[] { -1, 0, "2016-02-15", "2016-01-01", "2016-02-29" }, //
        };
    }

    @Override
    protected PeriodBuilder getBuilder() {
        return new NaturalMonthPeriodBuilder();
    }

    @Override
    protected PeriodBuilder getBuilder(String startDate) {
        return new NaturalMonthPeriodBuilder(startDate);
    }

}
