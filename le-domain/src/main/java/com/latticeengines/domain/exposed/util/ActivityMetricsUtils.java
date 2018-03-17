package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsUtils {

    public final static String SEPARATOR = "__"; // Need to be different with
                                                  // TimeFilter.SEPARATOR
    public final static String HEADER = "AM_"; // Avro field name only allows to
                                               // start with letter or "_"

    @SuppressWarnings("serial")
    private static Map<String, String> metricsDisplayNames = new HashMap<String, String>() {
        {
            put(InterfaceName.Margin.name(), "Percentage margin");
            put(InterfaceName.SpendChange.name(), "Percentage spend change");
            put(InterfaceName.ShareOfWallet.name(), "Percentage share of wallet");
            put(InterfaceName.AvgSpendOvertime.name(), "Average spend");
            put(InterfaceName.TotalSpendOvertime.name(), "Total spend");
            put(InterfaceName.HasPurchased.name(), "Has Purchased");
        }
    };

    public static String getNameWithPeriod(ActivityMetrics activityMetrics) {
        List<String> periodNames = new ArrayList<>();
        activityMetrics.getPeriodsConfig().forEach(config -> {
            periodNames.add(config.getPeriodRangeName());
        });
        return String.join(SEPARATOR, periodNames) + SEPARATOR + activityMetrics.getMetrics();
    }

    public static String getFullName(ActivityMetrics activityMetrics, String activityId) {
        return HEADER + activityId.replace("_", "") + SEPARATOR + getNameWithPeriod(activityMetrics);
    }

    public static String getFullName(String nameWithPeriod, String activityId) {
        return HEADER + activityId.replace("_", "") + SEPARATOR + nameWithPeriod;
    }

    public static String getActivityIdFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(0, fullName.indexOf(SEPARATOR));
    }

    public static String getMetricsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.lastIndexOf(SEPARATOR) + SEPARATOR.length());
    }
    
    public static String getPeriodsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.indexOf(SEPARATOR) + SEPARATOR.length(), fullName.lastIndexOf(SEPARATOR));
    }

    // <DisplayName, SecondDisplayName>
    public static Pair<String, String> getDisplayNamesFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        String metrics = getMetricsFromFullName(fullName);
        String displayName = metricsDisplayNames.get(metrics);
        String period = getPeriodsFromFullName(fullName);
        // Currently only SpendChange has 2 periods.
        // Only show first period in display name per PM's requirement
        if (period.contains(SEPARATOR)) {
            period = period.substring(0, period.indexOf(SEPARATOR));
        }
        displayName += periodStrToDisplayName(period);
        // TODO: populate second display name
        return Pair.of(displayName, null);
    }

    private static String periodStrToDisplayName(String period) {
        String[] strs = period.split(TimeFilter.SEPARATOR);
        if (strs[0].equalsIgnoreCase(ComparisonType.EVER.name())) {
            return "";
        } else if (strs[0].equalsIgnoreCase(ComparisonType.WITHIN.name())) {
            return String.format(" in last %s %s%s", strs[2], strs[1].toLowerCase(),
                    Integer.valueOf(strs[2]) > 1 ? "s" : "");
        } else {
            // Currently only has EVER and WITHIN case
            throw new UnsupportedOperationException(strs[0] + " comparison type is not fully supported yet");
        }

    }
}
