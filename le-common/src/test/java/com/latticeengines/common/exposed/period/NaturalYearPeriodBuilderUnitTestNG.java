package com.latticeengines.common.exposed.period;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;

public class NaturalYearPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void testPeriodCount(String startDate, String endDate, int period) {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new NaturalYearPeriodBuilder();
        } else {
            builder = new NaturalYearPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(actualPeriod, period);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
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

    @Test(groups = "unit", dataProvider = "periodRangeProvider")
    public void testConvertToDateRange(int period1, int period2, String firstDate, String startDate, String endDate) {
        PeriodBuilder builder;
        if (StringUtils.isBlank(firstDate)) {
            builder = new NaturalYearPeriodBuilder();
        } else {
            builder = new NaturalYearPeriodBuilder(firstDate);
        }
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(period1, period2);
        Assert.assertEquals(dateRange.getLeft(), LocalDate.parse(startDate));
        Assert.assertEquals(dateRange.getRight(), LocalDate.parse(endDate));
    }

    @DataProvider(name = "periodRangeProvider")
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

}
