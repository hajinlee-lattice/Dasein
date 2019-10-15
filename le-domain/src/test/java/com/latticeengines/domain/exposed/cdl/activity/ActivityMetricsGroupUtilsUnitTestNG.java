package com.latticeengines.domain.exposed.cdl.activity;

import java.util.Collections;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class ActivityMetricsGroupUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "groupNameProvider")
    public void testFromGroupNameToGroupIdBase(String groupName, String expected) {
        String generated = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "timeFilterProvider")
    public void testTimeFilterToTimeRangeInGroupId(TimeFilter timeFilter, String expected) {
        String generated = ActivityMetricsGroupUtils.timeFilterToTimeRangeTemplate(timeFilter);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "timeFilterDescriptionProvider")
    public void testTimeFilterToDescription(String timeFilter, String tmpl, String expected) {
        String generated = ActivityMetricsGroupUtils.timeRangeTmplToDescription(timeFilter, tmpl);
        Assert.assertEquals(generated, expected);
    }

    @Test(groups = "unit", dataProvider = "sourceMediumProvider")
    public void testSourceMediumDisplayName(String timeRange, Map<String, Object> SourceMedium, String expected) {
        Assert.assertEquals(ActivityMetricsGroupUtils.generateSourceMediumDisplayName(timeRange, SourceMedium), expected);
    }

    @DataProvider(name = "groupNameProvider")
    public Object[][] groupNameProvider() {
        return new Object[][]{
                {"short", "sxx"}, //
                {"group name with medium length", "gnwml"}, //
                {"a very very very long group name", "avvvlg"}, //
                {"__a _2 3 _B_ c__ d_2_", "a23Bcd"}
        };
    }

    @DataProvider(name = "timeFilterProvider")
    public Object[][] timeFilterProvider() {
        return new Object[][] {
                { new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.toString(),
                        Collections.singletonList(2)), "w_2_w" },
                { new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.toString(),
                        Collections.singletonList(10)), "w_10_w" } };
    }

    @DataProvider(name = "timeFilterDescriptionProvider")
    public Object[][] timeFilterDescriptionProvider() {
        return new Object[][] {
                { "w_2_w", "${operator} ${params?join(\"_\")} ${period}", "in last 2 week" },
                { "b_2_4_w", "${operator} ${params?join(\" and \")} ${period}", "between 2 and 4 week" } };
    }

    @DataProvider(name = "sourceMediumProvider")
    public Object[][] sourceMediumProvider() {
        return new Object[][]{
                {"in last 2 week", Collections.singletonMap("SourceMedium", "__others__"), "Visit in last 2 week by all other sources"}, //
                {"in last 2 week", Collections.singletonMap("SourceMedium", "w.le.c"), "Visit in last 2 week from w.le.c"}
        };
    }
}
