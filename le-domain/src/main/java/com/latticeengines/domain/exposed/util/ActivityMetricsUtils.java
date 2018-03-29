package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsUtils {

    public final static String SEPARATOR = "__";
    public final static String SUB_SEPARATOR = "_";
    public final static String HEADER = "AM_"; // Avro field name only allows to start with letter or "_"

    @SuppressWarnings("serial")
    private static Map<InterfaceName, String> metricsDisplayNames = new HashMap<InterfaceName, String>() {
        {
            put(InterfaceName.Margin, "% margin");
            put(InterfaceName.SpendChange, "% spend change");
            put(InterfaceName.ShareOfWallet, "% share of wallet");
            put(InterfaceName.AvgSpendOvertime, "Average spend");
            put(InterfaceName.TotalSpendOvertime, "Total spend");
            put(InterfaceName.HasPurchased, "Has Purchased");
        }
    };

    @SuppressWarnings("serial")
    private static Map<InterfaceName, String> metricsAbbr = new HashMap<InterfaceName, String>() {
        {
            put(InterfaceName.Margin, "MG");
            put(InterfaceName.SpendChange, "SC");
            put(InterfaceName.ShareOfWallet, "SW");
            put(InterfaceName.AvgSpendOvertime, "AS");
            put(InterfaceName.TotalSpendOvertime, "TS");
            put(InterfaceName.HasPurchased, "HP");
        }
    };

    @SuppressWarnings("serial")
    private static Map<String, InterfaceName> metricsAbbrRev = new HashMap<String, InterfaceName>() {
        {
            put("MG", InterfaceName.Margin);
            put("SC", InterfaceName.SpendChange);
            put("SW", InterfaceName.ShareOfWallet);
            put("AS", InterfaceName.AvgSpendOvertime);
            put("TS", InterfaceName.TotalSpendOvertime);
            put("HP", InterfaceName.HasPurchased);
        }
    };

    @SuppressWarnings("serial")
    private static Map<String, String> periodAbbr = new HashMap<String, String> () {
        {
            put(PeriodStrategy.Template.Year.name(), "Y");
            put(PeriodStrategy.Template.Quarter.name(), "Q");
            put(PeriodStrategy.Template.Month.name(), "M");
            put(PeriodStrategy.Template.Week.name(), "W");
        }
    };

    @SuppressWarnings("serial")
    private static Map<String, PeriodStrategy.Template> periodAbbrRev = new HashMap<String, PeriodStrategy.Template>() {
        {
            put("Y", PeriodStrategy.Template.Year);
            put("Q", PeriodStrategy.Template.Quarter);
            put("M", PeriodStrategy.Template.Month);
            put("W", PeriodStrategy.Template.Week);
        }
    };

    public static String getNameWithPeriod(ActivityMetrics activityMetrics) {
        List<String> periodNames = new ArrayList<>();
        activityMetrics.getPeriodsConfig().forEach(config -> {
            periodNames.add(getPeriodRangeName(config));
        });
        return String.join(SEPARATOR, periodNames) + SEPARATOR + metricsAbbr.get(activityMetrics.getMetrics());
    }

    public static String getFullName(ActivityMetrics activityMetrics, String activityId) {
        return HEADER + activityId + SEPARATOR + getNameWithPeriod(activityMetrics);
    }

    public static String getFullName(String nameWithPeriod, String activityId) {
        return HEADER + activityId + SEPARATOR + nameWithPeriod;
    }

    public static String getActivityIdFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(0, fullName.indexOf(SEPARATOR));
    }

    public static String getDepivotedAttrNameFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.indexOf(SEPARATOR) + 2);
    }

    public static InterfaceName getMetricsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return metricsAbbrRev.get(fullName.substring(fullName.lastIndexOf(SEPARATOR) + SEPARATOR.length()));
    }
    
    public static String getPeriodsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.indexOf(SEPARATOR) + SEPARATOR.length(), fullName.lastIndexOf(SEPARATOR));
    }

    // <DisplayName, SecondDisplayName>
    public static Pair<String, String> getDisplayNamesFromFullName(String fullName, String currentTxnDate,
            List<PeriodStrategy> strategies) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        InterfaceName metrics = getMetricsFromFullName(fullName);
        String displayName = metricsDisplayNames.get(metrics);
        String period = getPeriodsFromFullName(fullName);
        displayName += periodStrToDisplayName(period, metrics);
        String secDisplayName = periodStrToSecondDisplayName(period, metrics, currentTxnDate, strategies);
        return Pair.of(displayName, secDisplayName);
    }

    public static String getHasPurchasedAbbr() {
        return metricsAbbr.get(InterfaceName.HasPurchased);
    }

    private static String periodStrToSecondDisplayName(String period, InterfaceName metrics, String currentTxnDate,
            List<PeriodStrategy> strategies) {
        if (metrics == InterfaceName.HasPurchased || StringUtils.isBlank(currentTxnDate)
                || CollectionUtils.isEmpty(strategies)) {
            return null;
        }
        if (metrics == InterfaceName.SpendChange) {
            String strs[] = period.split(SEPARATOR);
            if (strs[0].split(SUB_SEPARATOR).length == 2) {
                period = strs[0];
            } else {
                period = strs[1];
            }
        }

        String[] strs = period.split(SUB_SEPARATOR);
        TimeFilterTranslator timeFilterTranslator = new TimeFilterTranslator(strategies, currentTxnDate);
        TimeFilter timeFilter = TimeFilter.within(Integer.valueOf(strs[1]), periodAbbrRev.get(strs[0]).name());
        List<Object> translatedTxnDateRange = timeFilterTranslator.translate(timeFilter).getValues();
        return "(" + translatedTxnDateRange.get(0).toString() + " to " + translatedTxnDateRange.get(1).toString() + ")";
    }

    private static String periodStrToDisplayName(String period, InterfaceName metrics) {
        if (metrics == InterfaceName.HasPurchased) {
            return "";
        }
        if (metrics == InterfaceName.SpendChange) {
            String strs[] = period.split(SEPARATOR);
            if (strs[0].split(SUB_SEPARATOR).length == 2) {
                period = strs[0];
            } else {
                period = strs[1];
            }
        }
        return getDisplayNameForWithinComp(period);
    }

    private static String getDisplayNameForWithinComp(String period) {
        String[] strs = period.split(SUB_SEPARATOR);
        return String.format(" in last %s %s%s", strs[1], periodAbbrRev.get(strs[0]).name().toLowerCase(),
                Integer.valueOf(strs[1]) > 1 ? "s" : "");
    }

    private static String getPeriodRangeName(TimeFilter timeFilter) {
        if (timeFilter.getRelation() == ComparisonType.EVER) {
            return ComparisonType.EVER.name();
        }
        List<String> strs = new ArrayList<>();
        strs.add(periodAbbr.get(timeFilter.getPeriod()));
        timeFilter.getValues().forEach(value -> {
            strs.add(String.valueOf(value));
        });
        return String.join(SUB_SEPARATOR, strs);
    }
}
