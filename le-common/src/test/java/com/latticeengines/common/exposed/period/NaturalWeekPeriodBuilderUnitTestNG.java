package com.latticeengines.common.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class NaturalWeekPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void testPeriodCount(String startDate, String endDate, int period) throws Exception {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new NaturalWeekPeriodBuilder();
        } else {
            builder = new NaturalWeekPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(new Integer(actualPeriod), new Integer(period));
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
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

    @Test(groups = "unit", dataProvider = "periodRangeProvider")
    public void testConvertToDateRange(int period1, int period2, String firstDate, String startDate, String endDate) {
        PeriodBuilder builder;
        if (StringUtils.isBlank(firstDate)) {
            builder = new NaturalWeekPeriodBuilder();
        } else {
            builder = new NaturalWeekPeriodBuilder(firstDate);
        }
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(period1, period2);
        Assert.assertEquals(dateRange.getLeft(), LocalDate.parse(startDate));
        Assert.assertEquals(dateRange.getRight(), LocalDate.parse(endDate));
    }

    @DataProvider(name = "periodRangeProvider")
    public Object[][] providePeriodRanges() {
        return new Object[][] { //
                new Object[] { 0, 0, null, "1999-12-26", "2000-01-01" }, //
                new Object[] { 0, 1, null, "1999-12-26", "2000-01-08" }, //
                new Object[] { 1, 2, null, "2000-01-02", "2000-01-15" }, //
                new Object[] { -1, 0, null, "1999-12-19", "2000-01-01" }, //
                new Object[] { -2, -1, null, "1999-12-12", "1999-12-25" } //
        };
    }

}
