package com.latticeengines.objectapi.util;

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
        TimeFilter within1Month = new TimeFilter( //
                ComparisonType.WITHIN, TimeFilter.Period.Month.name(), Collections.singletonList(1));
        TimeFilter within3Month = new TimeFilter( //
                ComparisonType.WITHIN, TimeFilter.Period.Month.name(), Collections.singletonList(3));
        TimeFilter within2Quarter = new TimeFilter( //
                ComparisonType.WITHIN, TimeFilter.Period.Quarter.name(), Collections.singletonList(2));
        TimeFilter betweenDates = new TimeFilter( //
                ComparisonType.BETWEEN, TimeFilter.Period.Date.name(), Arrays.asList("2018-02-01", "2019-01-01"));
        return new Object[][] { //
                { TimeFilter.ever(), null }, //
                { within1Month, Pair.of("2018-01-01", "2018-01-31") }, //
                { within3Month, Pair.of("2017-11-01", "2018-01-31") }, //
                { within2Quarter, Pair.of("2017-07-01", "2017-12-31") }, //
                { betweenDates, Pair.of("2018-02-01", "2019-01-01") }, //
        };
    }

    private TimeFilterTranslator getTranslator() {
        return new TimeFilterTranslator(PeriodStrategy.NATURAL_PERIODS, "2018-02-17");
    }

}
