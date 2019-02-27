package com.latticeengines.domain.exposed.period;


import org.testng.annotations.DataProvider;

public class NaturalYearPeriodBuilderUnitTestNG extends NaturalPeriodBuilderUnitTestNGBase {

    @DataProvider(name = PERIOD_ID_DATA_PROVIDER)
    protected Object[][] providePeriodIds() {
        return new Object[][] { //
                new Object[] { "2016-02-05", "2016-01-01", 0 }, //
                new Object[] { "2016-02-05", "2015-12-31", -1 }, //
                new Object[] { "2016-12-31", "2017-01-30", 1 }, //
                new Object[] { "2015-01-01", "2017-12-30", 2 }, //
                new Object[] { "2017-01-01", "2016-12-30", -1 }, //
                new Object[] { "2017-04-15", "2016-08-10", -1 }, //
                new Object[] { null, "2001-08-10", 1 }, //
                new Object[] { null, "2017-12-30", 17 }, //
        };
    }

    @DataProvider(name = PERIOD_RANGE_DATA_PROVIDER)
    public Object[][] providePeriodRanges() {
        return new Object[][] { //
                new Object[] { 0, 0, null, "2000-01-01", "2000-12-31" }, //
                new Object[] { 0, 1, null, "2000-01-01", "2001-12-31" }, //
                new Object[] { -1, 0, null, "1999-01-01", "2000-12-31" }, //
                new Object[] { -3, -1, null, "1997-01-01", "1999-12-31" }, //
                new Object[] { 3, 7, null, "2003-01-01", "2007-12-31" }, //
                new Object[] { 0, 0, "2018-02-16", "2018-01-01", "2018-12-31" }, //
                new Object[] { 0, 1, "2018-02-16", "2018-01-01", "2019-12-31" }, //
                new Object[] { 3, 7, "2018-02-16", "2021-01-01", "2025-12-31" }, //
        };
    }

    @Override
    protected PeriodBuilder getBuilder() {
        return new NaturalYearPeriodBuilder();
    }

    @Override
    protected PeriodBuilder getBuilder(String startDate) {
        return new NaturalYearPeriodBuilder(startDate);
    }

}
