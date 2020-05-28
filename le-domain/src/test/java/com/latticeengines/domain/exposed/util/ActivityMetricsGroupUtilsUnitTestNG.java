package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template.Day;
import static com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template.Week;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class ActivityMetricsGroupUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "validAttrNames")
    public void testParseAttrName(String attrName, List<String> expected) {
        try {
            List<String> tokens = ActivityMetricsGroupUtils.parseAttrName(attrName);
            Assert.assertEquals(CollectionUtils.size(tokens), 3);
            Assert.assertEquals(tokens.get(0), expected.get(0));
            Assert.assertEquals(tokens.get(1), expected.get(1));
            Assert.assertEquals(tokens.get(2), expected.get(2));
        } catch (ParseException e) {
            Assert.fail("Should not fail to parse " + attrName, e);
        }
    }

    @DataProvider(name = "validAttrNames")
    public Object[][] provideValidAttrNames() {
        return new Object[][]{
                { "am_twv__pathpatternid__w_2_w", asList("twv", "pathpatternid", "w_2_w") },
                { "am_twv__id1_id2__b_2_3_w", asList("twv", "id1_id2", "b_2_3_w") },
        };
    }

    @Test(groups = "unit", dataProvider = "invalidAttrNames", expectedExceptions = ParseException.class)
    public void testParseInvalidAttrName(String attrName) throws ParseException {
        ActivityMetricsGroupUtils.parseAttrName(attrName);
    }

    @Test(groups = "unit", dataProvider = "groupNameProvider")
    public void testFromGroupNameToGroupIdBase(String groupName, String expected) {
        String generated = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "timeFilterProvider")
    public void testTimeFilterToTimeRangeTmpl(TimeFilter timeFilter, String expected) {
        String generated = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "timeRangeDescriptionProvider")
    public void testTimeFilterToDescription(String timeRange, String expected) {
        String generated = ActivityMetricsGroupUtils.timeRangeTmplToDescription(timeRange);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "timeRangeToPeriodOnlyProvider")
    public void testTimeRangeToPeriodOnlyProvider(String timeRange, int offset, String expected) {
        String generated = ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, offset);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "timeRangeToTimeFilterProvider", dependsOnMethods = "testTimeFilterToTimeRangeTmpl")
    public void testTimeRangeToTimeFilter(String timeRange, TimeFilter expected) {
        TimeFilter generated = ActivityMetricsGroupUtils.timeRangeTmplToTimeFilter(timeRange);
        String translated = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(generated);
        String expectedTmpl = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(expected);
        Assert.assertEquals(translated, expectedTmpl);
    }

    @Test(groups = "unit", dataProvider = "activityTimeRange")
    private void testActivityTimeRangeToTimeFilters(ActivityTimeRange timeRange,
            @NotNull List<TimeFilter> expectedTimeFilters) {
        List<TimeFilter> timeFilters = ActivityMetricsGroupUtils.toTimeFilters(timeRange);
        Assert.assertNotNull(timeFilters, "should return non-null list of time filters");
        Assert.assertEquals(timeFilters.size(), expectedTimeFilters.size());
        Assert.assertEquals(new HashSet<>(timeFilters), new HashSet<>(expectedTimeFilters));
    }

    @Test(groups = "unit", dataProvider = "unsupportedActivityTimeRange", expectedExceptions = UnsupportedOperationException.class)
    private void testUnsupportedActivityTimeRangeToFilters(@NotNull ActivityTimeRange timeRange) {
        ActivityMetricsGroupUtils.toTimeFilters(timeRange);
    }

    @DataProvider(name = "groupNameProvider")
    public Object[][] groupNameProvider() {
        return new Object[][]{
                {"short", "sxx"}, //
                {"group name with medium length", "gnwml"}, //
                {"a very very very long group name", "avvvlg"}, //
                {"__a _2 3 _B_ c__ d_2_", "a23bcd"}, //
                {"Total Web Visit", "twv"}, //
                {"Web Visit By sOuRcE Medium", "wvbsm"}
        };
    }

    @DataProvider(name = "timeFilterProvider")
    public Object[][] timeFilterProvider() {
        return new Object[][]{
                {new TimeFilter(ComparisonType.WITHIN, Week.toString(),
                        singletonList(2)), "w_2_w" },
                {new TimeFilter(ComparisonType.WITHIN, Week.toString(),
                        singletonList(10)), "w_10_w" },
                {TimeFilter.ever(Week.toString()), "ev_w"}
        };
    }

    @DataProvider(name = "timeRangeDescriptionProvider")
    public Object[][] timeFilterDescriptionProvider() {
        return new Object[][]{
                {"w_1_w", "Last 1 week"},
                {"w_2_w", "Last 2 weeks"},
                {"b_2_4_w", "Between 2 and 4 weeks"},
                {"ev_w", ""} // all periods filters don't need description
        };
    }

    @DataProvider(name = "timeRangeToPeriodOnlyProvider")
    public Object[][] timeRangeToPeriodOnlyProvider() {
        return new Object[][]{
                {"w_1_w", 0, "1 week"},
                {"w_10_w", 1, "11 weeks"},
                {"w_3_w", 9, "12 weeks"}
        };
    }

    @DataProvider(name = "timeRangeToTimeFilterProvider")
    public Object[][] timeRangeToTimeFilterProvider() {
        return new Object[][]{ //
                { "w_2_w",
                        new TimeFilter(ComparisonType.WITHIN, Week.toString(),
                                singletonList(2)) }, //
                { "w_4_w",
                        new TimeFilter(ComparisonType.WITHIN, Week.toString(),
                                singletonList(4)) }, //
                { "b_2_4_w",
                        new TimeFilter(ComparisonType.BETWEEN, Week.toString(), asList(2, 4)) }, //
                { "b_4_12_w", new TimeFilter(ComparisonType.BETWEEN, Week.toString(),
                        asList(4, 12)) },
                {"ev_w", TimeFilter.ever(Week.toString())}, //
        };
    }

    @DataProvider(name = "invalidAttrNames")
    public Object[][] provideInvalidAttrNames() {
        return new Object[][]{ //
                {"foo"}, //
                {"foo bar"}, //
                {"twv__pathpatternid__w_2_w"}, //
                {"am__twv__id1_id2__b_2_3_w"}, //
        };
    }

    @DataProvider(name = "activityTimeRange")
    private Object[][] activityTimeRangeTestData() {
        return new Object[][] { //
                { null, emptyList() }, //
                { new ActivityTimeRange(), emptyList() }, //
                { withinTimeRange(null, null), emptyList() }, //
                { withinTimeRange(new String[] { Day.name() }, null), emptyList() }, //
                { withinTimeRange(new String[0], null), emptyList() }, //
                { withinTimeRange(new String[0], new Integer[0]), emptyList() }, //
                { withinTimeRange(null, new Integer[] { 1, 2, 4 }), emptyList() }, //
                { withinTimeRange(new String[] { Day.name() }, new Integer[] { 1, 2, 4 }),
                        asList(withinTimeFilter(Day.name(), 1), withinTimeFilter(Day.name(), 2),
                                withinTimeFilter(Day.name(), 4)) }, //
                { withinTimeRange(new String[] { Day.name() }, new Integer[] { 1 }),
                        singletonList(withinTimeFilter(Day.name(), 1)) }, //
                { withinTimeRange(new String[] { Day.name(), Week.name() }, new Integer[] { 1 }),
                        asList(withinTimeFilter(Day.name(), 1), withinTimeFilter(Week.name(), 1)) }, //
                { withinTimeRange(new String[] { Day.name(), Week.name() }, new Integer[] { 1, 2 }),
                        asList(withinTimeFilter(Day.name(), 1), withinTimeFilter(Week.name(), 1),
                                withinTimeFilter(Day.name(), 2), withinTimeFilter(Week.name(), 2)) }, //
                { everTimeRange(new String[] { Week.name() }), Collections.singletonList(TimeFilter.ever(Week.name())) } //
        }; //
    }

    @DataProvider(name = "unsupportedActivityTimeRange")
    private Object[][] unsupportedTimeRangeTestData() {
        return new Object[][] { //
                { timeRangeWithOperator(ComparisonType.AFTER) }, //
                { timeRangeWithOperator(ComparisonType.BEFORE) }, //
                { timeRangeWithOperator(ComparisonType.BETWEEN_DATE) } //
        }; //
    }

    private TimeFilter withinTimeFilter(String period, int val) {
        return new TimeFilter(ComparisonType.WITHIN, period, singletonList(val));
    }

    private ActivityTimeRange timeRangeWithOperator(ComparisonType type) {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(type);

        // dummy non-null values
        timeRange.setPeriods(singleton(Day.name()));
        timeRange.setParamSet(ImmutableSet.of(singletonList(1), singletonList(2)));
        return timeRange;
    }

    private ActivityTimeRange withinTimeRange(String[] periods, Integer[] params) {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.WITHIN);
        if (periods != null) {
            timeRange.setPeriods(Arrays.stream(periods).collect(Collectors.toSet()));
        }
        if (params != null) {
            timeRange.setParamSet(Arrays.stream(params) //
                    .map(Collections::singletonList).collect(Collectors.toSet()));
        }
        return timeRange;
    }

    private ActivityTimeRange everTimeRange(String[] periods) {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.EVER);
        timeRange.setPeriods(Arrays.stream(periods).collect(Collectors.toSet()));
        return timeRange;
    }
}
