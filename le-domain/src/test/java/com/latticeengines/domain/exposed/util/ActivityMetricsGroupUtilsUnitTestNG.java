package com.latticeengines.domain.exposed.util;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class ActivityMetricsGroupUtilsUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsGroupUtilsUnitTestNG.class);

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
                {"am_twv__pathpatternid__w_2_w", Arrays.asList("twv", "pathpatternid", "w_2_w")},
                {"am_twv__id1_id2__b_2_3_w", Arrays.asList("twv", "id1_id2", "b_2_3_w")},
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

    @Test(groups = "unit", dataProvider = "timeRangeToTimeFilterProvider", dependsOnMethods = "testTimeFilterToTimeRangeTmpl")
    public void testTimeRangeToTimeFilter(String timeRange, TimeFilter expected) {
        TimeFilter generated = ActivityMetricsGroupUtils.timeRangeTmplToTimeFilter(timeRange);
        String translated = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(generated);
        String expectedTmpl = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(expected);
        Assert.assertEquals(translated, expectedTmpl);
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
                {new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.toString(),
                        Collections.singletonList(2)), "w_2_w"},
                {new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.toString(),
                        Collections.singletonList(10)), "w_10_w"}};
    }

    @DataProvider(name = "timeRangeDescriptionProvider")
    public Object[][] timeFilterDescriptionProvider() {
        return new Object[][]{
                {"w_2_w", "in last 2 week"},
                {"b_2_4_w", "between 2 and 4 week"}};
    }

    @DataProvider(name = "timeRangeToTimeFilterProvider")
    public Object[][] timeRangeToTimeFilterProvider() {
        return new Object[][]{ //
                {"w_2_w", new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.toString(), Collections.singletonList(2))}, //
                {"w_4_w", new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.toString(), Collections.singletonList(4))}, //
                {"b_2_4_w", new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Week.toString(), Arrays.asList(2, 4))}, //
                {"b_4_12_w", new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Week.toString(), Arrays.asList(4, 12))}
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
}
