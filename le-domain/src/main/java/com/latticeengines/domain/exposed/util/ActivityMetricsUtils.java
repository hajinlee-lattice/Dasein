package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsUtils {

    private final static String SEPARATOR = "__";
    private final static String SUB_SEPARATOR = "_";
    private final static String HEADER = "AM_"; // Avro field name only allows
                                                // to start with letter or "_"

    private static Map<InterfaceName, String> metricsDisplayNames = new HashMap<>();
    private static Map<InterfaceName, String> metricsAbbr = new HashMap<>();
    private static Map<InterfaceName, String> metricsDescription = new HashMap<>();
    private static Map<String, InterfaceName> metricsAbbrRev = new HashMap<>();
    private static Map<String, String> periodAbbr = new HashMap<>();
    private static Map<String, PeriodStrategy.Template> periodAbbrRev = new HashMap<>();
    private static Map<InterfaceName, NullMetricsImputation> nullImputation = new HashMap<>();
    private static Map<InterfaceName, Set<ComparisonType>> comparisonType = new HashMap<>();
    private static Map<InterfaceName, Integer> maxCnt = new HashMap<>();
    private static Set<String> validPeriods = new HashSet<>(Arrays.asList( //
            PeriodStrategy.Template.Year.name(), //
            PeriodStrategy.Template.Quarter.name(), //
            PeriodStrategy.Template.Month.name(), //
            PeriodStrategy.Template.Week.name() //
    ));

    static {
        metricsDisplayNames.put(InterfaceName.Margin, "% Margin");
        metricsDisplayNames.put(InterfaceName.SpendChange, "% Spend Change");
        metricsDisplayNames.put(InterfaceName.ShareOfWallet, "% Share of Wallet");
        metricsDisplayNames.put(InterfaceName.AvgSpendOvertime, "Average Spend");
        metricsDisplayNames.put(InterfaceName.TotalSpendOvertime, "Total Spend");
        metricsDisplayNames.put(InterfaceName.HasPurchased, "Has Purchased");

        metricsAbbr.put(InterfaceName.Margin, "MG");
        metricsAbbr.put(InterfaceName.SpendChange, "SC");
        metricsAbbr.put(InterfaceName.ShareOfWallet, "SW");
        metricsAbbr.put(InterfaceName.AvgSpendOvertime, "AS");
        metricsAbbr.put(InterfaceName.TotalSpendOvertime, "TS");
        metricsAbbr.put(InterfaceName.HasPurchased, "HP");

        metricsDescription.put(InterfaceName.Margin,
                "This curated attribute is calculated by analyzing cost of sell & revenue for a given product of a given account in the specified time window. "
                        + "The insights are useful to drive sales & marketing campaigns for the accounts where the profit margins are below expected levels.");
        metricsDescription.put(InterfaceName.SpendChange,
                "This curated attribute is calculated by comparing average spend for a given product of a given account in the specified time window with that of the range in prior time window.");
        metricsDescription.put(InterfaceName.ShareOfWallet,
                "This curated attribute is calculated by comparing spend ratio for a given product of a given account with that of other accounts in the same segment. "
                        + "This insights are useful to drive sales & marketing campaigns for the accounts where the share of wallet is below the desired range.");
        metricsDescription.put(InterfaceName.AvgSpendOvertime,
                "This curated attribute is calculated by aggregating average spend for a given product of a given account over a specified time window.");
        metricsDescription.put(InterfaceName.TotalSpendOvertime,
                "This curated attribute is calculated by aggregating total spend for a given product of a given account over a specified time window.");
        metricsDescription.put(InterfaceName.HasPurchased,
                "Indicates if this product ever was purchased by this account.");

        metricsAbbrRev.put("MG", InterfaceName.Margin);
        metricsAbbrRev.put("SC", InterfaceName.SpendChange);
        metricsAbbrRev.put("SW", InterfaceName.ShareOfWallet);
        metricsAbbrRev.put("AS", InterfaceName.AvgSpendOvertime);
        metricsAbbrRev.put("TS", InterfaceName.TotalSpendOvertime);
        metricsAbbrRev.put("HP", InterfaceName.HasPurchased);

        periodAbbr.put(PeriodStrategy.Template.Year.name(), "Y");
        periodAbbr.put(PeriodStrategy.Template.Quarter.name(), "Q");
        periodAbbr.put(PeriodStrategy.Template.Month.name(), "M");
        periodAbbr.put(PeriodStrategy.Template.Week.name(), "W");

        periodAbbrRev.put("Y", PeriodStrategy.Template.Year);
        periodAbbrRev.put("Q", PeriodStrategy.Template.Quarter);
        periodAbbrRev.put("M", PeriodStrategy.Template.Month);
        periodAbbrRev.put("W", PeriodStrategy.Template.Week);

        nullImputation.put(InterfaceName.Margin, NullMetricsImputation.NULL);
        nullImputation.put(InterfaceName.SpendChange, NullMetricsImputation.ZERO);
        nullImputation.put(InterfaceName.ShareOfWallet, NullMetricsImputation.NULL);
        nullImputation.put(InterfaceName.AvgSpendOvertime, NullMetricsImputation.ZERO);
        nullImputation.put(InterfaceName.TotalSpendOvertime, NullMetricsImputation.ZERO);
        nullImputation.put(InterfaceName.HasPurchased, NullMetricsImputation.FALSE);

        comparisonType.put(InterfaceName.Margin,
                new HashSet<>(Arrays.asList(ComparisonType.WITHIN)));
        comparisonType.put(InterfaceName.SpendChange,
                new HashSet<>(Arrays.asList(ComparisonType.WITHIN, ComparisonType.BETWEEN)));
        comparisonType.put(InterfaceName.ShareOfWallet,
                new HashSet<>(Arrays.asList(ComparisonType.WITHIN)));
        comparisonType.put(InterfaceName.AvgSpendOvertime,
                new HashSet<>(Arrays.asList(ComparisonType.WITHIN)));
        comparisonType.put(InterfaceName.TotalSpendOvertime,
                new HashSet<>(Arrays.asList(ComparisonType.WITHIN)));
        comparisonType.put(InterfaceName.HasPurchased,
                new HashSet<>(Arrays.asList(ComparisonType.EVER)));

        maxCnt.put(InterfaceName.Margin, 1);
        maxCnt.put(InterfaceName.SpendChange, 1);
        maxCnt.put(InterfaceName.ShareOfWallet, 1);
        maxCnt.put(InterfaceName.AvgSpendOvertime, 5);
        maxCnt.put(InterfaceName.TotalSpendOvertime, 5);
        maxCnt.put(InterfaceName.HasPurchased, 1);
    }

    private ActivityMetricsUtils() {
    }

    public static boolean isHasPurchasedAttr(String fullName) {
        return fullName.startsWith(ActivityMetricsUtils.HEADER) && fullName.endsWith(
                ActivityMetricsUtils.SEPARATOR + ActivityMetricsUtils.getHasPurchasedAbbr());
    }

    public static boolean isSpendChangeAttr(String fullName) {
        return fullName.startsWith(ActivityMetricsUtils.HEADER) && fullName.endsWith(
                ActivityMetricsUtils.SEPARATOR + ActivityMetricsUtils.getSpendChangeAttr());
    }

    public static String getNameWithPeriod(ActivityMetrics activityMetrics) {
        List<String> periodNames = new ArrayList<>();
        activityMetrics.getPeriodsConfig().forEach(config -> {
            periodNames.add(getPeriodRangeName(config));
        });
        return String.join(SEPARATOR, periodNames) + SEPARATOR
                + metricsAbbr.get(activityMetrics.getMetrics());
    }

    public static String getFullName(ActivityMetrics activityMetrics, String prefix) {
        return HEADER + prefix + SEPARATOR + getNameWithPeriod(activityMetrics);
    }

    public static String getFullName(String nameWithPeriod, String prefix) {
        return HEADER + prefix + SEPARATOR + nameWithPeriod;
    }

    public static String getProductIdFromFullName(String fullName) {
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
        return metricsAbbrRev
                .get(fullName.substring(fullName.lastIndexOf(SEPARATOR) + SEPARATOR.length()));
    }

    public static NullMetricsImputation getNullImputation(String fullName) {
        return nullImputation.get(getMetricsFromFullName(fullName));
    }

    public static String getPeriodsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.indexOf(SEPARATOR) + SEPARATOR.length(),
                fullName.lastIndexOf(SEPARATOR));
    }

    public static String getDescriptionFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        InterfaceName metrics = getMetricsFromFullName(fullName);
        return metricsDescription.get(metrics);
    }

    public static boolean isDeprecated(String fullName, List<ActivityMetrics> metrics) {
        String depivotedAttrName = getDepivotedAttrNameFromFullName(fullName);
        for (ActivityMetrics m : metrics) {
            if (depivotedAttrName.equals(getNameWithPeriod(m))) {
                if (m.isEOL()) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    public static String getMetricsDisplayName(InterfaceName metrics) {
        return metricsDisplayNames.get(metrics);
    }

    // <DisplayName, SecondDisplayName>
    public static Pair<String, String> getDisplayNamesFromFullName(String fullName,
            String evaluationDate, List<PeriodStrategy> strategies) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        InterfaceName metrics = getMetricsFromFullName(fullName);
        String displayName = metricsDisplayNames.get(metrics);
        String period = getPeriodsFromFullName(fullName);
        displayName += periodStrToDisplayName(period, metrics);
        String secDisplayName = periodStrToSecondDisplayName(period, metrics, evaluationDate,
                strategies);
        return Pair.of(displayName, secDisplayName);
    }

    public static String getHasPurchasedAbbr() {
        return metricsAbbr.get(InterfaceName.HasPurchased);
    }

    public static String getSpendChangeAttr() {
        return metricsAbbr.get(InterfaceName.SpendChange);
    }

    private static String periodStrToSecondDisplayName(String period, InterfaceName metrics,
            String currentTxnDate, List<PeriodStrategy> strategies) {
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
        TimeFilterTranslator timeFilterTranslator = new TimeFilterTranslator(strategies,
                currentTxnDate);
        TimeFilter timeFilter = TimeFilter.within(Integer.valueOf(strs[1]),
                periodAbbrRev.get(strs[0]).name());
        List<Object> translatedTxnDateRange = timeFilterTranslator.translate(timeFilter)
                .getValues();
        return "(" + translatedTxnDateRange.get(0).toString() + " to "
                + translatedTxnDateRange.get(1).toString() + ")";
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
        return String.format(" in last %s %s%s", strs[1],
                periodAbbrRev.get(strs[0]).name().toLowerCase(),
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

    public static boolean isValidMetrics(List<ActivityMetrics> metrics) {
        if (CollectionUtils.isEmpty(metrics)) {
            return true;
        }
        Map<InterfaceName, Integer> cnts = new HashMap<>();
        Set<String> metricsIds = new HashSet<>();
        for (ActivityMetrics m : metrics) {
            isValidMetrics(m);
            if (!m.isEOL()) {
                if (!cnts.containsKey(m.getMetrics())) {
                    cnts.put(m.getMetrics(), 0);
                }
                cnts.put(m.getMetrics(), cnts.get(m.getMetrics()) + 1);
            }
            metricsIds.add(ActivityMetricsUtils.getNameWithPeriod(m));
        }
        cnts.forEach((m, cnt) -> {
            if (cnt > maxCnt.get(m)) {
                throw new LedpException(LedpCode.LEDP_40032,
                        new String[] { String.format("Maximum for metrics %s is %d, but found %d",
                                m, maxCnt.get(m), cnt) });
            }
        });
        if (metricsIds.size() != metrics.size()) {
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { "Metrics cannot be duplicate" });
        }
        return true;
    }

    public static boolean isValidMetrics(ActivityMetrics metrics) {
        switch (metrics.getMetrics()) {
            case Margin:
            case ShareOfWallet:
            case TotalSpendOvertime:
            case AvgSpendOvertime:
                isValidPeriodConfig(metrics.getMetrics(), metrics.getPeriodsConfig(), 1);
                isValidComparisonType(metrics.getMetrics(),
                        metrics.getPeriodsConfig().get(0).getRelation());
                isValidPeriodValue(metrics.getMetrics(), metrics.getPeriodsConfig().get(0));
                break;
            case SpendChange:
                isValidPeriodConfig(metrics.getMetrics(), metrics.getPeriodsConfig(), 2);
                isValidComparisonTypes(metrics.getMetrics(), metrics.getPeriodsConfig());
                isValidPeriodValue(metrics.getMetrics(), metrics.getPeriodsConfig().get(0));
                isValidPeriodValue(metrics.getMetrics(), metrics.getPeriodsConfig().get(1));
                break;
            case HasPurchased:
                isValidPeriodConfig(metrics.getMetrics(), metrics.getPeriodsConfig(), 1);
                isValidComparisonType(metrics.getMetrics(),
                        metrics.getPeriodsConfig().get(0).getRelation());
                break;
            default:
                throw new LedpException(LedpCode.LEDP_40032,
                        new String[] { metrics.getMetrics() + " metrics is not supported" });
        }
        return true;
    }

    private static boolean isValidPeriodConfig(InterfaceName metricsName,
            List<TimeFilter> timeFilters, int expectedCnt) {
        if (CollectionUtils.isEmpty(timeFilters) || timeFilters.size() != expectedCnt) {
            throw new LedpException(LedpCode.LEDP_40032, new String[] {
                    metricsName + " metrics should have " + expectedCnt + " period config" });
        }
        Set<String> periods = new HashSet<>();
        for (TimeFilter timeFilter : timeFilters) {
            if (!validPeriods.contains(timeFilter.getPeriod())) {
                throw new LedpException(LedpCode.LEDP_40032,
                        new String[] { "Unknown period: " + timeFilter.getPeriod() });
            }
            periods.add(timeFilter.getPeriod());
        }
        if (periods.size() > 1) {
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { metricsName + " metrics should have consistent period name" });
        }
        return true;
    }

    private static boolean isValidPeriodValue(InterfaceName metricsName, TimeFilter timeFilter) {
        Map<ComparisonType, Integer> expectedCnt = new HashMap<>();
        expectedCnt.put(ComparisonType.WITHIN, 1);
        expectedCnt.put(ComparisonType.BETWEEN, 2);
        if (CollectionUtils.isEmpty(timeFilter.getValues())
                || timeFilter.getValues().size() != expectedCnt.get(timeFilter.getRelation())) {
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { metricsName + " metrics should have "
                            + expectedCnt.get(timeFilter.getRelation()) + " period values" });
        }
        for (Object val : timeFilter.getValues()) {
            try {
                if ((Integer) val <= 0) {
                    throw new LedpException(LedpCode.LEDP_40032, new String[] {
                            metricsName + " metrics should have positive period values" });
                }
            } catch (Exception ex) {
                throw new LedpException(LedpCode.LEDP_40032,
                        new String[] { "Fail to parse period value " + String.valueOf(val)
                                + " for metrics " + metricsName });
            }
        }
        return true;
    }

    private static boolean isValidComparisonType(InterfaceName metricsName, ComparisonType type) {
        if (type == null || !comparisonType.get(metricsName).contains(type)) {
            List<String> expectedTypes = new ArrayList<>();
            comparisonType.get(metricsName).forEach(ct -> {
                expectedTypes.add(ct.name());
            });
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { String.format(
                            "%s metrics should have comparison type as %s but found %s",
                            metricsName, String.join(",", expectedTypes), type) });
        }
        return true;
    }

    private static boolean isValidComparisonTypes(InterfaceName metricsName,
            List<TimeFilter> timeFilters) {
        Set<ComparisonType> types = new HashSet<>();
        timeFilters.forEach(pc -> {
            types.add(pc.getRelation());
        });
        if (CollectionUtils.isEmpty(types)
                || comparisonType.get(metricsName).size() != types.size()) {
            List<String> expectedTypes = new ArrayList<>();
            comparisonType.get(metricsName).forEach(ct -> {
                expectedTypes.add(ct.name());
            });
            List<String> actualTypes = new ArrayList<>();
            types.forEach(t -> {
                actualTypes.add(t.name());
            });
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { String.format(
                            "%s metrics should have comparison type as %s but found %s",
                            metricsName, String.join(",", expectedTypes),
                            String.join(",", actualTypes)) });
        }
        return true;
    }

    public static List<Number> insertZeroBndForSpendChangeBkt(List<Number> bounds) {
        if (CollectionUtils.isEmpty(bounds)) {
            return bounds;
        }
        if (bounds.get(0).doubleValue() < 0 && bounds.get(bounds.size() - 1).doubleValue() > 0) {
            for (int i = 0; i < bounds.size(); i++) {
                if (bounds.get(i).doubleValue() > 0) {
                    if (bounds.get(i - 1).doubleValue() < 0) {
                        bounds.add(i, 0);
                    }
                    break;
                }
            }
        }
        return bounds;
    }

    // For testing purpose
    public static List<ActivityMetrics> fakePurchaseMetrics(Tenant tenant) {
        ActivityMetrics margin = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        margin.setMetrics(InterfaceName.Margin);
        margin.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        ActivityMetrics shareOfWallet = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        shareOfWallet.setMetrics(InterfaceName.ShareOfWallet);
        shareOfWallet.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        ActivityMetrics spendChange = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        spendChange.setMetrics(InterfaceName.SpendChange);
        spendChange.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Quarter.name()),
                        TimeFilter.between(2, 3, PeriodStrategy.Template.Quarter.name())));

        ActivityMetrics avgSpendOvertime = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        avgSpendOvertime.setMetrics(InterfaceName.AvgSpendOvertime);
        avgSpendOvertime.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Quarter.name())));

        return Arrays.asList(margin, shareOfWallet, spendChange, avgSpendOvertime);
    }

    public static List<ActivityMetrics> fakeUpdatedPurchaseMetrics(Tenant tenant) {
        ActivityMetrics totalSpendOvertime = createFakedMetrics(tenant,
                ActivityType.PurchaseHistory);
        totalSpendOvertime.setMetrics(InterfaceName.TotalSpendOvertime);
        totalSpendOvertime.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Year.name())));

        return Arrays.asList(totalSpendOvertime);
    }

    private static ActivityMetrics createFakedMetrics(Tenant tenant, ActivityType type) {
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setType(type);
        metrics.setTenant(tenant);
        metrics.setEOL(false);
        metrics.setDeprecated(null);
        metrics.setCreated(new Date());
        metrics.setUpdated(metrics.getCreated());
        return metrics;
    }
}
