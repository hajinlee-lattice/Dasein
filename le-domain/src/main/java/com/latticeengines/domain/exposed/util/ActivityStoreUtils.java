package com.latticeengines.domain.exposed.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.query.ComparisonType;

/**
 * General activity store helpers
 */
public class ActivityStoreUtils {

    private ActivityStoreUtils() {
    }

    /**
     * Transform activity store specific pattern into regular expression. This is to
     * support some use cases such as wildcard (*) and can break some regex.
     *
     * @param activityStorePattern
     *            activity store specific pattern (mostly regex)
     * @return transformed regular expression
     */
    public static String modifyPattern(String activityStorePattern) {
        if (StringUtils.isBlank(activityStorePattern)) {
            return activityStorePattern;
        }

        // replace all * (that is not already .*) to .* to support wildcard
        return activityStorePattern.replaceAll("(?<!\\.)\\*", ".*");
    }

    /*-
     * default time range for metrics group
     * - last 2, 4, 8, 12 weeks
     */
    public static ActivityTimeRange defaultTimeRange() {
        Set<List<Integer>> paramSet = new HashSet<>();
        paramSet.add(Collections.singletonList(2));
        paramSet.add(Collections.singletonList(4));
        paramSet.add(Collections.singletonList(8));
        paramSet.add(Collections.singletonList(12));
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(ComparisonType.WITHIN);
        timeRange.setPeriods(Collections.singleton(PeriodStrategy.Template.Week.name()));
        timeRange.setParamSet(paramSet);
        return timeRange;
    }
}
