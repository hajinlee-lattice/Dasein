package com.latticeengines.common.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NaturalQuarterPeriodBuilderUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void testPeriodCount(String startDate, String endDate, int period) {
        PeriodBuilder builder;
        if (StringUtils.isBlank(startDate)) {
            builder = new NaturalQuarterPeriodBuilder();
        } else {
            builder = new NaturalQuarterPeriodBuilder(startDate);
        }
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(actualPeriod, period);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
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

    @Test(groups = "unit", dataProvider = "periodRangeProvider")
    public void testConvertToDateRange(int period1, int period2, String firstDate, String startDate, String endDate) {
        PeriodBuilder builder;
        if (StringUtils.isBlank(firstDate)) {
            builder = new NaturalQuarterPeriodBuilder();
        } else {
            builder = new NaturalQuarterPeriodBuilder(firstDate);
        }
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(period1, period2);
        Assert.assertEquals(dateRange.getLeft(), LocalDate.parse(startDate));
        Assert.assertEquals(dateRange.getRight(), LocalDate.parse(endDate));
    }

    @DataProvider(name = "periodRangeProvider")
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
}
