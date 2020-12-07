package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.FilterOptions;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class ActivityStoreUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "modifyPatternTestData")
    private void testModifyPattern(String ptn, String expectedModifiedPtn, boolean isValidPtn) {
        String regexStr = ActivityStoreUtils.modifyPattern(ptn);
        Assert.assertEquals(regexStr, expectedModifiedPtn);

        Assert.assertEquals(isValidRegex(regexStr), isValidPtn,
                "Pattern: " + ptn + " Modified Pattern: " + expectedModifiedPtn
                        + " doesn't have the expected validity as regex");
    }

    @Test(groups = "unit", dataProvider = "filterProviderForOptionDisplayName")
    private void testFilterOptionDisplayName(TimeFilter filter, String expected) {
        Assert.assertEquals(ActivityStoreUtils.filterOptionDisplayName(filter), expected);
    }

    private boolean isValidRegex(String regexStr) {
        try {
            Pattern.compile(regexStr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test(groups = "unit")
    private void testGenericFilters() {
        List<String> expected = Arrays.asList( //
                "any", //
                "wi_0_w", //
                "1 week", //
                "2 weeks", //
                "4 weeks", //
                "8 weeks", //
                "12 weeks" //
        );
        FilterOptions options = ActivityStoreUtils.genericFilterOptions();
        Assert.assertEquals(options.getLabel(), "Timeframe");
        Assert.assertEquals(options.getOptions().stream().map(FilterOptions.Option::getValue).collect(Collectors.toList()), expected);
    }

    @Test(groups = "unit", dataProvider = "filterTagsProvider")
    private void testGetFilterTagsFromTimeRange(String timeRange, List<String> expected) {
        List<String> actual = ActivityStoreUtils.getFilterTagsFromTimeRange(timeRange);
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "modifyPatternTestData")
    private Object[][] modifyPatternTestData() {
        return new Object[][] { //
                { "", "", true }, //
                { "   ", "   ", true }, //
                { "*.**", ".*.*.*", true }, //
                { "https://google.com/*", "https://google.com/.*", true }, //
                { "*google.com/about/*", ".*google.com/about/.*", true }, //
                { "https://google.com/.*/*.html", "https://google.com/.*/.*.html", true }, //
                { "https://google.com/.*/*\\.html", "https://google.com/.*/.*\\.html", true }, // escape . to be safer
                /* invalid pattern */
                { null, null, false }, //
                { "[", "[", false }, //
                { "*[*", ".*[.*", false }, //
        };
    }

    @DataProvider(name = "filterProviderForOptionDisplayName")
    private Object[][] filterProviderForOptionDisplayName() {
        return new Object[][] {
                {TimeFilter.within(1, PeriodStrategy.Template.Week.name()), "Last 1 Weeks"},
                {TimeFilter.within(2, PeriodStrategy.Template.Week.name()), "Last 2 Weeks"},
                {TimeFilter.withinInclude(0, PeriodStrategy.Template.Week.name()), "Current week till today"},
                {TimeFilter.withinInclude(1, PeriodStrategy.Template.Week.name()), "1 week till today"},
                {TimeFilter.withinInclude(2, PeriodStrategy.Template.Week.name()), "2 weeks till today"},
        };
    }

    @DataProvider(name = "filterTagsProvider")
    public static Object[][] filterTagsProvider() {
        return new Object[][] { //
                {"wi_0_w", Arrays.asList("wi_0_w", "any")}, //
                {"wi_1_w", Arrays.asList("wi_1_w", "any", "1 week")}, //
                {"wi_8_w", Arrays.asList("wi_8_w", "any", "8 weeks")}, //
                {"w_8_w", Arrays.asList("w_8_w", "any", "8 weeks")} //
        };
    }
}
