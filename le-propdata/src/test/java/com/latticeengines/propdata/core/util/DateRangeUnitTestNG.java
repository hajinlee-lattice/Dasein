package com.latticeengines.propdata.core.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.propdata.core.util.DateRange;

public class DateRangeUnitTestNG {

    @Test(groups = {"unit"})
    public void testTimeZone() throws Exception{
        DateRange range = new DateRange("2015-01-01", "2015-01-02");
        Assert.assertEquals("[2015-01-01 00:00:00 UTC - 2015-01-02 00:00:00 UTC]", range.toString());
    }

    @Test(groups = {"unit"}, dataProvider = "numOfPeriodsDataProvider")
    public void testSplitDateRangeByNumberOfPeriods(DateRange dateRange, int numOfPeriods, List<DateRange> ranges) {
        List<DateRange> result = dateRange.splitByNumOfPeriods(numOfPeriods);
        Assert.assertEquals(result, ranges);
    }

    @DataProvider(name = "numOfPeriodsDataProvider")
    Object[][] numOfPeriodsDataProvider() {
        return new Object[][]{
                {
                        new DateRange("2013-01-01", "2013-01-31"), 10,
                        Arrays.asList(
                                new DateRange("2013-01-01", "2013-01-04"),
                                new DateRange("2013-01-04", "2013-01-07"),
                                new DateRange("2013-01-07", "2013-01-10"),
                                new DateRange("2013-01-10", "2013-01-13"),
                                new DateRange("2013-01-13", "2013-01-16"),
                                new DateRange("2013-01-16", "2013-01-19"),
                                new DateRange("2013-01-19", "2013-01-22"),
                                new DateRange("2013-01-22", "2013-01-25"),
                                new DateRange("2013-01-25", "2013-01-28"),
                                new DateRange("2013-01-28", "2013-01-31")
                        )
                },
                {
                        new DateRange("2014-02-14", "2014-02-20"), 6,
                        Arrays.asList(
                                new DateRange("2014-02-14", "2014-02-15"),
                                new DateRange("2014-02-15", "2014-02-16"),
                                new DateRange("2014-02-16", "2014-02-17"),
                                new DateRange("2014-02-17", "2014-02-18"),
                                new DateRange("2014-02-18", "2014-02-19"),
                                new DateRange("2014-02-19", "2014-02-20")
                        )
                },
                {
                        new DateRange("2015-01-21", "2015-09-20"), 1,
                        Collections.singletonList(new DateRange("2015-01-21", "2015-09-20"))
                }
        };
    }

    @Test(groups = {"unit"}, dataProvider = "daysInPeriodDataProvider")
    public void testSplitDateRangeByDaysInPeriod(DateRange dateRange, int daysInPeriod, List<DateRange> ranges) {
        List<DateRange> result = dateRange.splitByDaysPerPeriod(daysInPeriod);
        Assert.assertEquals(result, ranges);
    }

    @DataProvider(name = "daysInPeriodDataProvider")
    Object[][] daysInPeriodDataProvider() {
        return new Object[][]{
                {
                        new DateRange("2014-02-01", "2014-02-20"), 19,
                        Collections.singletonList(new DateRange("2014-02-01", "2014-02-20"))
                },
                {
                        new DateRange("2014-02-01", "2014-02-20"), 20,
                        Collections.singletonList(new DateRange("2014-02-01", "2014-02-20"))
                },
                {
                        new DateRange("2014-02-01", "2014-02-20"), 7,
                        Arrays.asList(
                                new DateRange("2014-02-01", "2014-02-08"),
                                new DateRange("2014-02-08", "2014-02-15"),
                                new DateRange("2014-02-15", "2014-02-20")
                        )
                },
                {
                        new DateRange("2013-11-03", "2013-12-29"), 28,
                        Arrays.asList(
                                new DateRange("2013-11-03", "2013-12-01"),
                                new DateRange("2013-12-01", "2013-12-29")
                        )
                }
        };
    }

}
