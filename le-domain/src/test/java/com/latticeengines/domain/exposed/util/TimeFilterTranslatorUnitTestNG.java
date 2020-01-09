package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class TimeFilterTranslatorUnitTestNG {

    @Test(groups = "unit")
    public void testTranslate() {
        TimeFilterTranslator translator = getTranslator();
        translator.getSpecifiedValues().get(ComparisonType.LATEST_DAY)
                .put(new AttributeLookup(BusinessEntity.Account, "attr1"), Arrays.asList(12345L, 12346L));
        TimeFilter tf = translator.translate(TimeFilter.latestDay(),
                new AttributeLookup(BusinessEntity.Account, "attr1"));
        Assert.assertEquals(tf.getRelation(), ComparisonType.BETWEEN);
        Assert.assertNotNull(tf.getValues());
        Assert.assertEquals(tf.getValues().size(), 2);
        System.out.println(tf.getValues());
    }

    @Test(groups = "unit", dataProvider = "timeFilterProvider")
    public void testTranslateRangeToDate(TimeFilter timeFilter, Pair<String, String> expected) {
        TimeFilterTranslator translator = getTranslator();
        Pair<String, String> actual = translator.periodIdRangeToDateRange(timeFilter.getPeriod(),
                translator.translateRange(timeFilter));
        System.out.println(actual);
        if (expected == null) {
            Assert.assertNull(actual);
        } else {
            Assert.assertNotNull(actual);
            Assert.assertEquals(actual.getLeft(), expected.getLeft());
            Assert.assertEquals(actual.getRight(), expected.getRight());
        }
    }

    @DataProvider(name = "timeFilterProvider")
    public Object[][] provideTimeFilters() {
        TimeFilter currentMonth = new TimeFilter(//
                ComparisonType.IN_CURRENT_PERIOD, PeriodStrategy.Template.Month.name(), Collections.emptyList());
        TimeFilter within1Month = new TimeFilter(//
                ComparisonType.WITHIN, PeriodStrategy.Template.Month.name(), Collections.singletonList(1));
        TimeFilter within3Month = new TimeFilter(//
                ComparisonType.WITHIN, PeriodStrategy.Template.Month.name(), Collections.singletonList(3));
        TimeFilter within2Quarter = new TimeFilter(//
                ComparisonType.WITHIN, PeriodStrategy.Template.Quarter.name(), Collections.singletonList(2));
        TimeFilter within2QuarterIncludeCurrent = new TimeFilter(//
                ComparisonType.WITHIN_INCLUDE, PeriodStrategy.Template.Quarter.name(), Collections.singletonList(2));
        TimeFilter prior1Month = new TimeFilter(//
                ComparisonType.PRIOR, PeriodStrategy.Template.Month.name(), Collections.singletonList(1));
        TimeFilter prior3Month = new TimeFilter(//
                ComparisonType.PRIOR, PeriodStrategy.Template.Month.name(), Collections.singletonList(3));
        TimeFilter between1And3Month = new TimeFilter(//
                ComparisonType.BETWEEN, PeriodStrategy.Template.Month.name(), Arrays.asList(1, 3));
        TimeFilter betweenDates = new TimeFilter(//
                ComparisonType.BETWEEN_DATE, PeriodStrategy.Template.Date.name(),
                Arrays.asList("2018-02-01", "2019-01-01"));
        TimeFilter beforeDate = new TimeFilter(//
                ComparisonType.BEFORE, PeriodStrategy.Template.Date.name(), Collections.singletonList("2018-02-01"));
        TimeFilter afterDate = new TimeFilter(//
                ComparisonType.AFTER, PeriodStrategy.Template.Date.name(), Collections.singletonList("2018-02-01"));
        TimeFilter last1Day = new TimeFilter(//
                ComparisonType.LAST, PeriodStrategy.Template.Day.name(), Collections.singletonList(1));
        TimeFilter last7Days = new TimeFilter(//
                ComparisonType.LAST, PeriodStrategy.Template.Day.name(), Collections.singletonList(7));
        TimeFilter last1DayV2 = TimeFilter.last(1, null);
        TimeFilter last7DaysV2 = TimeFilter.last(7, PeriodStrategy.Template.Day.name());
        TimeFilter last60Days = new TimeFilter(//
                ComparisonType.LAST, PeriodStrategy.Template.Day.name(), Collections.singletonList(60));

        return new Object[][] { //
                { TimeFilter.ever(), null }, //
                { TimeFilter.isEmpty(), null }, //
                { TimeFilter.latestDay(), null }, //
                { currentMonth, Pair.of("2018-02-01", "2018-02-28") }, //
                { within1Month, Pair.of("2018-01-01", "2018-01-31") }, //
                { within3Month, Pair.of("2017-11-01", "2018-01-31") }, //
                { within2Quarter, Pair.of("2017-07-01", "2017-12-31") }, //
                { within2QuarterIncludeCurrent, Pair.of("2017-07-01", "2018-03-31") }, //
                { prior1Month, Pair.of(null, "2017-12-31") }, //
                { prior3Month, Pair.of(null, "2017-10-31") }, //
                { between1And3Month, Pair.of("2017-11-01", "2018-01-31") }, //
                { betweenDates, Pair.of("2018-02-01", "2019-01-01") }, //
                { beforeDate, Pair.of(null, "2018-02-01") }, //
                { afterDate, Pair.of("2018-02-01", null) }, //
                { last1Day, Pair.of("2018-02-17", "2018-02-17") }, //
                { last7Days, Pair.of("2018-02-11", "2018-02-17") }, //
                { last1DayV2, Pair.of("2018-02-17", "2018-02-17") }, //
                { last7DaysV2, Pair.of("2018-02-11", "2018-02-17") }, //
                { last60Days, Pair.of("2017-12-20", "2018-02-17") }, // PLS-15955
        };
    }

    @Test(groups = "unit", dataProvider = "timeRangeAndTimeFilterProvider")
    public void testTranslateRangeToPeriodId(TimeFilter timeFilter, Pair<Integer, Integer> expected) {
        TimeFilterTranslator translator = getTranslator();
        Pair<Integer, Integer> actual = translator.translateRange(timeFilter);
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "timeRangeAndTimeFilterProvider")
    public Object[][] timeRangeAndTimeFilterProvider() {
        TimeFilter last7Day = new TimeFilter(//
                ComparisonType.LAST, PeriodStrategy.Template.Day.name(), Collections.singletonList(7));
        TimeFilter last1Day = new TimeFilter(//
                ComparisonType.LAST, PeriodStrategy.Template.Day.name(), Collections.singletonList(1));
        TimeFilter betweenDates = new TimeFilter(//
                ComparisonType.BETWEEN_DATE, PeriodStrategy.Template.Date.name(),
                Arrays.asList("2018-02-01", "2019-01-01"));
        TimeFilter last60Day = new TimeFilter(//
                ComparisonType.LAST, PeriodStrategy.Template.Day.name(), Collections.singletonList(60));
        return new Object[][] {
                { last7Day,
                        Pair.of(DateTimeUtils.dateToDayPeriod("2018-02-11"),
                                DateTimeUtils.dateToDayPeriod("2018-02-17")) }, //
                { last1Day,
                        Pair.of(DateTimeUtils.dateToDayPeriod("2018-02-17"),
                                DateTimeUtils.dateToDayPeriod("2018-02-17")) }, //
                { betweenDates,
                        Pair.of(DateTimeUtils.dateToDayPeriod("2018-02-01"),
                                DateTimeUtils.dateToDayPeriod("2019-01-01")) }, //
                { last60Day, Pair.of(DateTimeUtils.dateToDayPeriod("2017-12-20"),
                        DateTimeUtils.dateToDayPeriod("2018-02-17")) }, // PLS-15955
        };
    }

    private TimeFilterTranslator getTranslator() {
        return new TimeFilterTranslator(PeriodStrategy.NATURAL_PERIODS, "2018-02-17");
    }
}
