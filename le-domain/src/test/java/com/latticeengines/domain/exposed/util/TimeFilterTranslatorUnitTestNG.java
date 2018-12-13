package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class TimeFilterTranslatorUnitTestNG {

    @Test(groups = "unit", dataProvider = "timeFilterProvider")
    public void test(TimeFilter timeFilter, Pair<String, String> expected) {
        TimeFilterTranslator translator = getTranslator();
        Pair<String, String> actual = translator.translateRange(timeFilter);
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
        return new Object[][] { //
                { TimeFilter.ever(), null }, //
                { TimeFilter.isEmpty(), null }, //
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
        };
    }

    private TimeFilterTranslator getTranslator() {
        return new TimeFilterTranslator(PeriodStrategy.NATURAL_PERIODS, "2018-02-17");
    }

}
