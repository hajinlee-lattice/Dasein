package com.latticeengines.common.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NaturalMonthPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void testPeriodCount(String startDate, String endDate, int period) throws Exception {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new NaturalMonthPeriodBuilder();
        } else {
            builder = new NaturalMonthPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(new Integer(actualPeriod), new Integer(period));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
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

    @Test(groups = "unit", dataProvider = "periodRangeProvider")
    public void testConvertToDateRange(int period1, int period2, String firstDate, String startDate, String endDate) {
        PeriodBuilder builder;
        if (StringUtils.isBlank(firstDate)) {
            builder = new NaturalMonthPeriodBuilder();
        } else {
            builder = new NaturalMonthPeriodBuilder(firstDate);
        }
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(period1, period2);
        Assert.assertEquals(dateRange.getLeft(), LocalDate.parse(startDate));
        Assert.assertEquals(dateRange.getRight(), LocalDate.parse(endDate));
    }

    @DataProvider(name = "periodRangeProvider")
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
}
