package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsUtils {

    private static final String SEPARATOR = "__";
    private static final String SUB_SEPARATOR = "_";
    private static final String HEADER = "AM_"; // Avro field name only allows
                                                // to start with letter or "_"

    // Metrics enum -> display name
    private static final Map<InterfaceName, String> METRICS_DISPLAY_NAMES = new HashMap<>();

    // Metrics enum -> abbreviation name
    private static final Map<InterfaceName, String> METRICS_ABBR = new HashMap<>();

    // Metrics enum -> description
    private static final Map<InterfaceName, String> METRICS_DESC = new HashMap<>();

    // Metrics abbreviation name -> enum
    private static final Map<String, InterfaceName> METRICS_ABBR_REV = new HashMap<>();

    // Period name -> period abbreviation
    private static final Map<String, String> PERIOD_ABBR = new HashMap<>();

    // Period abbreviation -> period template
    private static final Map<String, PeriodStrategy.Template> PERIOD_ABBR_REV = new HashMap<>();

    // Metrics enum -> null imputation strategy
    private static final Map<InterfaceName, NullMetricsImputation> NULL_IMPUTATION = new HashMap<>();

    // Metrics enum -> supported combinations of comparison types (one metrics
    // could have multiple supported combinations of comparison types -- eg. PM
    // updates period config for a metrics but we need to support old period
    // config to ensure backward compatibility, so value is a set; string in the
    // set is concatenated names of one combination of comparison types)
    private static final Map<InterfaceName, Set<String>> COMPARISON_TYPE = new HashMap<>();

    // Comparison type -> #period should be set to support this comparison type
    private static final Map<ComparisonType, Integer> COMPARISON_TYPE_PERIOD_CNTS = new HashMap<>();

    // Metrics enum -> maximum allowed count
    static final Map<InterfaceName, Integer> MAX_CNTS = new HashMap<>();

    // Valid period names
    private static final Set<String> VALID_PERIODS = new HashSet<>(Arrays.asList( //
            PeriodStrategy.Template.Year.name(), //
            PeriodStrategy.Template.Quarter.name(), //
            PeriodStrategy.Template.Month.name(), //
            PeriodStrategy.Template.Week.name() //
    ));

    static {
        METRICS_DISPLAY_NAMES.put(InterfaceName.Margin, "% Margin");
        METRICS_DISPLAY_NAMES.put(InterfaceName.SpendChange, "% Spend Change");
        METRICS_DISPLAY_NAMES.put(InterfaceName.ShareOfWallet, "% Share of Wallet");
        METRICS_DISPLAY_NAMES.put(InterfaceName.AvgSpendOvertime, "Average Spend");
        METRICS_DISPLAY_NAMES.put(InterfaceName.TotalSpendOvertime, "Total Spend");
        METRICS_DISPLAY_NAMES.put(InterfaceName.HasPurchased, "Has Purchased");
    }

    static {
        METRICS_ABBR.put(InterfaceName.Margin, "MG");
        METRICS_ABBR.put(InterfaceName.SpendChange, "SC");
        METRICS_ABBR.put(InterfaceName.ShareOfWallet, "SW");
        METRICS_ABBR.put(InterfaceName.AvgSpendOvertime, "AS");
        METRICS_ABBR.put(InterfaceName.TotalSpendOvertime, "TS");
        METRICS_ABBR.put(InterfaceName.HasPurchased, "HP");
    }

    static {
        METRICS_DESC.put(InterfaceName.Margin,
                "This curated attribute is calculated by analyzing cost of sell & revenue for a given product of a given account in the specified time window. "
                        + "The insights are useful to drive sales & marketing campaigns for the accounts where the profit margins are below expected levels.");
        METRICS_DESC.put(InterfaceName.SpendChange,
                "This curated attribute is calculated by comparing average spend for a given product of a given account in the specified time window with that of the range in prior time window.");
        METRICS_DESC.put(InterfaceName.ShareOfWallet,
                "This curated attribute is calculated by comparing spend ratio for a given product of a given account with that of other accounts in the same segment. "
                        + "This insights are useful to drive sales & marketing campaigns for the accounts where the share of wallet is below the desired range.");
        METRICS_DESC.put(InterfaceName.AvgSpendOvertime,
                "This curated attribute is calculated by aggregating average spend for a given product of a given account over a specified time window.");
        METRICS_DESC.put(InterfaceName.TotalSpendOvertime,
                "This curated attribute is calculated by aggregating total spend for a given product of a given account over a specified time window.");
        METRICS_DESC.put(InterfaceName.HasPurchased,
                "Indicates if this product ever was purchased by this account.");
    }

    static {
        METRICS_ABBR_REV.put("MG", InterfaceName.Margin);
        METRICS_ABBR_REV.put("SC", InterfaceName.SpendChange);
        METRICS_ABBR_REV.put("SW", InterfaceName.ShareOfWallet);
        METRICS_ABBR_REV.put("AS", InterfaceName.AvgSpendOvertime);
        METRICS_ABBR_REV.put("TS", InterfaceName.TotalSpendOvertime);
        METRICS_ABBR_REV.put("HP", InterfaceName.HasPurchased);
    }

    static {
        PERIOD_ABBR.put(PeriodStrategy.Template.Year.name(), "Y");
        PERIOD_ABBR.put(PeriodStrategy.Template.Quarter.name(), "Q");
        PERIOD_ABBR.put(PeriodStrategy.Template.Month.name(), "M");
        PERIOD_ABBR.put(PeriodStrategy.Template.Week.name(), "W");
    }

    static {
        PERIOD_ABBR_REV.put("Y", PeriodStrategy.Template.Year);
        PERIOD_ABBR_REV.put("Q", PeriodStrategy.Template.Quarter);
        PERIOD_ABBR_REV.put("M", PeriodStrategy.Template.Month);
        PERIOD_ABBR_REV.put("W", PeriodStrategy.Template.Week);
    }

    static {
        NULL_IMPUTATION.put(InterfaceName.Margin, NullMetricsImputation.NULL);
        NULL_IMPUTATION.put(InterfaceName.SpendChange, NullMetricsImputation.ZERO);
        NULL_IMPUTATION.put(InterfaceName.ShareOfWallet, NullMetricsImputation.NULL);
        NULL_IMPUTATION.put(InterfaceName.AvgSpendOvertime, NullMetricsImputation.ZERO);
        NULL_IMPUTATION.put(InterfaceName.TotalSpendOvertime, NullMetricsImputation.ZERO);
        NULL_IMPUTATION.put(InterfaceName.HasPurchased, NullMetricsImputation.FALSE);
    }

    static {
        COMPARISON_TYPE.put(InterfaceName.Margin,
                new HashSet<>(Arrays.asList(buildComparisonTypeLookupKey(ComparisonType.WITHIN))));
        COMPARISON_TYPE.put(InterfaceName.SpendChange,
                new HashSet<>(
                        Arrays.asList(buildComparisonTypeLookupKey(ComparisonType.WITHIN, ComparisonType.BETWEEN))));
        COMPARISON_TYPE.put(InterfaceName.ShareOfWallet,
                new HashSet<>(Arrays.asList(buildComparisonTypeLookupKey(ComparisonType.WITHIN))));
        COMPARISON_TYPE.put(InterfaceName.AvgSpendOvertime,
                new HashSet<>(Arrays.asList(buildComparisonTypeLookupKey(ComparisonType.WITHIN),
                        buildComparisonTypeLookupKey(ComparisonType.BETWEEN))));
        COMPARISON_TYPE.put(InterfaceName.TotalSpendOvertime,
                new HashSet<>(Arrays.asList(buildComparisonTypeLookupKey(ComparisonType.WITHIN),
                        buildComparisonTypeLookupKey(ComparisonType.BETWEEN))));
        COMPARISON_TYPE.put(InterfaceName.HasPurchased,
                new HashSet<>(Arrays.asList(buildComparisonTypeLookupKey(ComparisonType.EVER))));
    }

    static {
        COMPARISON_TYPE_PERIOD_CNTS.put(ComparisonType.WITHIN, 1);
        COMPARISON_TYPE_PERIOD_CNTS.put(ComparisonType.BETWEEN, 2);
    }

    static {
        MAX_CNTS.put(InterfaceName.Margin, 1);
        MAX_CNTS.put(InterfaceName.SpendChange, 1);
        MAX_CNTS.put(InterfaceName.ShareOfWallet, 1);
        MAX_CNTS.put(InterfaceName.AvgSpendOvertime, 5);
        MAX_CNTS.put(InterfaceName.TotalSpendOvertime, 5);
        MAX_CNTS.put(InterfaceName.HasPurchased, 1);
    }

    private ActivityMetricsUtils() {
    }

    /**
     * Check whether it is HasPurchase attr
     *
     * @param fullName
     * @return
     */
    public static boolean isHasPurchasedAttr(String fullName) {
        return StringUtils.isNotBlank(fullName) && fullName.startsWith(ActivityMetricsUtils.HEADER)
                && fullName.endsWith(ActivityMetricsUtils.SEPARATOR + ActivityMetricsUtils.getHasPurchasedAbbr());
    }

    /**
     * Check whether it is SpendChange attr
     *
     * @param fullName
     * @return
     */
    public static boolean isSpendChangeAttr(String fullName) {
        return fullName.startsWith(ActivityMetricsUtils.HEADER)
                && fullName.endsWith(ActivityMetricsUtils.SEPARATOR + ActivityMetricsUtils.getSpendChangeAbbr());
    }

    /**
     * Check whether it is TotalSpend attr
     *
     * @param fullName
     * @return
     */
    public static boolean isTotalSpendAttr(String fullName) {
        return fullName.startsWith(ActivityMetricsUtils.HEADER)
                && fullName.endsWith(ActivityMetricsUtils.SEPARATOR + ActivityMetricsUtils.getTotalSpendAbbr());
    }

    /**
     * Check whether it is AvgSpend attr
     *
     * @param fullName
     * @return
     */
    public static boolean isAvgSpendAttr(String fullName) {
        return fullName.startsWith(ActivityMetricsUtils.HEADER)
                && fullName.endsWith(ActivityMetricsUtils.SEPARATOR + ActivityMetricsUtils.getAvgSpendAbbr());
    }

    /**
     * @param activityMetrics
     * @return Format: PeriodAbbr_PeriodVal_PeriodAbbr_PeriodVal_..._MetricsAbbr
     */
    public static String getNameWithPeriod(ActivityMetrics activityMetrics) {
        List<String> periodNames = new ArrayList<>();
        activityMetrics.getPeriodsConfig().forEach(config -> {
            periodNames.add(getPeriodRangeName(config));
        });
        return String.join(SEPARATOR, periodNames) + SEPARATOR
                + METRICS_ABBR.get(activityMetrics.getMetrics());
    }


    /**
     * @param fullName
     * @return Format: PeriodAbbr_PeriodVal_PeriodAbbr_PeriodVal_..._MetricsAbbr
     */
    public static String getNameWithPeriodFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.indexOf(SEPARATOR) + 2);
    }

    /**
     * @param activityMetrics
     * @param prefix
     * @return Format: AM__{some prefix as ProductId etc}_PeriodAbbr_PeriodVal_PeriodAbbr_PeriodVal_..._MetricsAbbr
     */
    public static String getFullName(ActivityMetrics activityMetrics, String prefix) {
        return HEADER + prefix + SEPARATOR + getNameWithPeriod(activityMetrics);
    }

    /**
     * @param nameWithPeriod
     * @param prefix
     * @return Format: AM__{some prefix as ProductId etc}_PeriodAbbr_PeriodVal_PeriodAbbr_PeriodVal_..._MetricsAbbr
     */
    public static String getFullName(String nameWithPeriod, String prefix) {
        return HEADER + prefix + SEPARATOR + nameWithPeriod;
    }

    /**
     * Extract prefix like ProductId from full metrics name
     *
     * @param fullName
     * @return
     */
    public static String getProductIdFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(0, fullName.indexOf(SEPARATOR));
    }

    /**
     * Get metrics enum from full name
     *
     * @param fullName
     * @return
     */
    public static InterfaceName getMetricsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return METRICS_ABBR_REV
                .get(fullName.substring(fullName.lastIndexOf(SEPARATOR) + SEPARATOR.length()));
    }

    /**
     * Find null imputation strategy by full metrics name
     *
     * @param fullName
     * @return
     */
    public static NullMetricsImputation getNullImputation(String fullName) {
        return NULL_IMPUTATION.get(getMetricsFromFullName(fullName));
    }

    /**
     * Extract period names with values from full metrics name
     *
     * @param fullName
     * @return
     */
    public static String getPeriodsFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        fullName = fullName.substring(HEADER.length()); // remove header
        return fullName.substring(fullName.indexOf(SEPARATOR) + SEPARATOR.length(),
                fullName.lastIndexOf(SEPARATOR));
    }

    /**
     * Find metrics description by full name
     *
     * @param fullName
     * @return
     */
    public static String getDescriptionFromFullName(String fullName) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        InterfaceName metrics = getMetricsFromFullName(fullName);
        return METRICS_DESC.get(metrics);
    }

    /**
     * Check whether a metrics is deprecated
     *
     * @param fullName:
     *            full name of the metrics to check deprecation
     * @param metrics:
     *            current all the metrics configuration
     * @return
     */
    public static boolean isDeprecated(String fullName, List<ActivityMetrics> metrics) {
        String nameWithPeriod = getNameWithPeriodFromFullName(fullName);
        return metrics.stream() //
                .filter(ActivityMetrics::isEOL) //
                .map(m -> getNameWithPeriod(m)) //
                .anyMatch(nameWithPeriod::equals);
    }

    /**
     * Get metrics display name by metrics enum
     *
     * @param metrics
     * @return
     */
    public static String getMetricsDisplayName(InterfaceName metrics) {
        return METRICS_DISPLAY_NAMES.get(metrics);
    }


    /**
     * Get metrics display name and secondary display name
     *
     * @param fullName
     * @param evaluationDate
     * @param strategies
     * @return <DisplayName, SecondDisplayName>
     */
    public static Pair<String, String> getDisplayNamesFromFullName(String fullName,
            String evaluationDate, List<PeriodStrategy> strategies) {
        if (StringUtils.isBlank(fullName) || !fullName.contains(SEPARATOR)) {
            return null;
        }
        InterfaceName metrics = getMetricsFromFullName(fullName);
        String displayName = METRICS_DISPLAY_NAMES.get(metrics);
        String period = getPeriodsFromFullName(fullName);
        displayName += periodStrToDisplayName(period, metrics);
        String secDisplayName = periodStrToSecondDisplayName(period, metrics, evaluationDate,
                strategies);
        return Pair.of(displayName, secDisplayName);
    }

    /**
     * Get HasPurchased metrics abbr name
     *
     * @return
     */
    public static String getHasPurchasedAbbr() {
        return METRICS_ABBR.get(InterfaceName.HasPurchased);
    }

    /**
     * Get SpendChange metrics abbr name
     *
     * @return
     */
    public static String getSpendChangeAbbr() {
        return METRICS_ABBR.get(InterfaceName.SpendChange);
    }

    /**
     * Get TotalSpend metrics abbr name
     *
     * @return
     */
    public static String getTotalSpendAbbr() {
        return METRICS_ABBR.get(InterfaceName.TotalSpendOvertime);
    }

    /**
     * Get AvgSpend metrics abbr name
     *
     * @return
     */
    public static String getAvgSpendAbbr() {
        return METRICS_ABBR.get(InterfaceName.AvgSpendOvertime);
    }

    /**
     * Convert period names + values metrics to secondary display name
     *
     * @param period
     * @param metrics
     * @param currentTxnDate
     * @param strategies
     * @return
     */
    private static String periodStrToSecondDisplayName(String period, InterfaceName metrics,
            String currentTxnDate, List<PeriodStrategy> strategies) {
        // No display name for period EVER
        if (metrics == InterfaceName.HasPurchased || StringUtils.isBlank(currentTxnDate)
                || CollectionUtils.isEmpty(strategies)) {
            return null;
        }
        // For SpendChange, period display name only needs for last n periods
        // (within comparison type)
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
        TimeFilter timeFilter;
        // For TotalSpend & AvgSpend, comparison type could be either BETWEEN
        // (start from M25) or WITHIN (ensure backward compatibility)
        if ((metrics == InterfaceName.TotalSpendOvertime || metrics == InterfaceName.AvgSpendOvertime)
                && period.split(SUB_SEPARATOR).length == 3) {
            timeFilter = TimeFilter.between(Integer.valueOf(strs[1]), Integer.valueOf(strs[2]),
                    PERIOD_ABBR_REV.get(strs[0]).name());
        } else {
            timeFilter = TimeFilter.within(Integer.valueOf(strs[1]), PERIOD_ABBR_REV.get(strs[0]).name());
        }
        List<Object> translatedTxnDateRange = timeFilterTranslator.translate(timeFilter)
                .getValues();
        return "(" + translatedTxnDateRange.get(0).toString() + " to "
                + translatedTxnDateRange.get(1).toString() + ")";
    }

    /**
     * Convert period names + values metrics to display name
     *
     * @param period
     * @param metrics
     * @return
     */
    private static String periodStrToDisplayName(String period, InterfaceName metrics) {
        // No display name for period EVER
        if (metrics == InterfaceName.HasPurchased) {
            return "";
        }
        // For TotalSpend & AvgSpend, comparison type could be either BETWEEN
        // (start from M25) or WITHIN (ensure backward compatibility)
        if ((metrics == InterfaceName.TotalSpendOvertime || metrics == InterfaceName.AvgSpendOvertime)
                && period.split(SUB_SEPARATOR).length == 3) {
            return getDisplayNameForBetweenComp(period);
        }
        // For SpendChange, period display name only needs for last n periods
        // (within comparison type)
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

    /**
     * Get period display name for BETWEEN comparison type
     *
     * @param period
     * @return
     */
    private static String getDisplayNameForBetweenComp(String period) {
        String[] strs = period.split(SUB_SEPARATOR);
        return String.format(" in last %s to %s %ss", strs[1], strs[2],
                PERIOD_ABBR_REV.get(strs[0]).name().toLowerCase());
    }

    /**
     * Get period display name for WITHIN comparison type
     *
     * @param period
     * @return
     */
    private static String getDisplayNameForWithinComp(String period) {
        String[] strs = period.split(SUB_SEPARATOR);
        return String.format(" in last %s %s%s", strs[1],
                PERIOD_ABBR_REV.get(strs[0]).name().toLowerCase(),
                Integer.valueOf(strs[1]) > 1 ? "s" : "");
    }

    private static String getPeriodRangeName(TimeFilter timeFilter) {
        if (timeFilter.getRelation() == ComparisonType.EVER) {
            return ComparisonType.EVER.name();
        }
        List<String> strs = new ArrayList<>();
        strs.add(PERIOD_ABBR.get(timeFilter.getPeriod()));
        timeFilter.getValues().forEach(value -> {
            strs.add(String.valueOf(value));
        });
        return String.join(SUB_SEPARATOR, strs);
    }

    /**
     * To validate a list of metrics
     *
     * @param metrics
     * @return
     */
    public static boolean isValidMetrics(List<ActivityMetrics> metrics) {
        if (CollectionUtils.isEmpty(metrics)) {
            return true;
        }
        Map<InterfaceName, Integer> cnts = new HashMap<>();
        Set<String> metricsIds = new HashSet<>();
        Set<String> dupMetricsDisplayNames = new HashSet<>();
        for (ActivityMetrics m : metrics) {
            isValidMetrics(m);
            if (!m.isEOL()) {
                if (!cnts.containsKey(m.getMetrics())) {
                    cnts.put(m.getMetrics(), 0);
                }
                cnts.put(m.getMetrics(), cnts.get(m.getMetrics()) + 1);
            }
            if (metricsIds.contains(ActivityMetricsUtils.getNameWithPeriod(m))) {
                dupMetricsDisplayNames.add(getMetricsDisplayName(m.getMetrics()));
            } else {
                metricsIds.add(ActivityMetricsUtils.getNameWithPeriod(m));
            }
        }
        cnts.forEach((m, cnt) -> {
            if (cnt > MAX_CNTS.get(m)) {
                throw new LedpException(LedpCode.LEDP_40032,
                        new String[] { String.format("Maximum for metrics %s is %d, but found %d",
                                m, MAX_CNTS.get(m), cnt) });
            }
        });
        if (CollectionUtils.isNotEmpty(dupMetricsDisplayNames)) {
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { String.format(
                            "%s %s duplicate configurations. Remove duplicate configurations to save and proceed.",
                            String.join(",", dupMetricsDisplayNames),
                            dupMetricsDisplayNames.size() == 1 ? "has" : "have") });
        }
        return true;
    }

    /**
     * To validate a single metrics
     *
     * @param metrics
     * @return
     */
    public static boolean isValidMetrics(ActivityMetrics metrics) {
        switch (metrics.getMetrics()) {
        case Margin:
        case ShareOfWallet:
        case TotalSpendOvertime:
        case AvgSpendOvertime:
            isValidPeriodConfig(metrics.getMetrics(), metrics.getPeriodsConfig(), 1);
            isValidComparisonType(metrics.getMetrics(), metrics.getPeriodsConfig().get(0).getRelation());
            isValidPeriodValue(metrics.getMetrics(), metrics.getPeriodsConfig().get(0));
            break;
        case SpendChange:
            isValidPeriodConfig(metrics.getMetrics(), metrics.getPeriodsConfig(), 2);
            isValidComparisonTypes(metrics.getMetrics(), metrics.getPeriodsConfig());
            isValidPeriodValue(metrics.getMetrics(), metrics.getPeriodsConfig().get(0));
            isValidPeriodValue(metrics.getMetrics(), metrics.getPeriodsConfig().get(1));
            break;
        case HasPurchased:
            // HasPurchased use comparison type as EVER in time filter so no
            // need to validate period value as it is empty
            isValidPeriodConfig(metrics.getMetrics(), metrics.getPeriodsConfig(), 1);
            isValidComparisonType(metrics.getMetrics(), metrics.getPeriodsConfig().get(0).getRelation());
            break;
        default:
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { metrics.getMetrics() + " metrics is not supported" });
        }
        return true;
    }

    /**
     * Check whether number of time filters is expected
     *
     * Check whether period names are all valid as period name is weak typed
     *
     * Check whether period names are consistent among time filters
     *
     * @param metricsName
     * @param timeFilters
     * @param expectedCnt
     * @return
     */
    private static boolean isValidPeriodConfig(InterfaceName metricsName,
            List<TimeFilter> timeFilters, int expectedCnt) {
        if (CollectionUtils.isEmpty(timeFilters) || timeFilters.size() != expectedCnt) {
            throw new LedpException(LedpCode.LEDP_40032, new String[] {
                    metricsName + " metrics should have " + expectedCnt + " period config" });
        }
        Set<String> periods = new HashSet<>();
        for (TimeFilter timeFilter : timeFilters) {
            if (!VALID_PERIODS.contains(timeFilter.getPeriod())) {
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

    /**
     * Validate whether period value is compatible with comparison type
     *
     * @param metricsName
     * @param timeFilter
     * @return
     */
    private static boolean isValidPeriodValue(InterfaceName metricsName, TimeFilter timeFilter) {
        if (CollectionUtils.isEmpty(timeFilter.getValues())
                || timeFilter.getValues().size() != COMPARISON_TYPE_PERIOD_CNTS.get(timeFilter.getRelation())) {
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { metricsName + " metrics should have "
                            + COMPARISON_TYPE_PERIOD_CNTS.get(timeFilter.getRelation()) + " period values" });
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
        if (timeFilter.getRelation() == ComparisonType.BETWEEN) {
            try {
                if ((Integer) timeFilter.getValues().get(0) > (Integer) timeFilter.getValues().get(1)) {
                    throw new LedpException(LedpCode.LEDP_40032, new String[] {
                            metricsName + " metrics should have period start boundary <= end boundary" });
                }
            } catch (Exception ex) {
                throw new LedpException(LedpCode.LEDP_40032, new String[] {
                        "Fail to parse period value " + String.valueOf(timeFilter.getValues().get(0)) + " or "
                                + String.valueOf(timeFilter.getValues().get(1)) + " for metrics " + metricsName });
            }
        }
        return true;
    }

    /**
     * Validate comparison type in time filter for metrics with single time
     * filter
     *
     * @param metricsName
     * @param type
     * @return
     */
    private static boolean isValidComparisonType(InterfaceName metricsName, ComparisonType type) {
        if (type == null || !COMPARISON_TYPE.get(metricsName).contains(buildComparisonTypeLookupKey(type))) {
            List<String> expectedTypes = new ArrayList<>();
            COMPARISON_TYPE.get(metricsName).forEach(ct -> {
                expectedTypes.add(ct);
            });
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { String.format(
                            "%s metrics should have comparison type as %s but found %s",
                            metricsName, String.join(" or ", expectedTypes), type) });
        }
        return true;
    }

    /**
     * Validate comparison types in a list of time filters for metrics with
     * multiple time filters
     *
     * @param metricsName
     * @param timeFilters
     * @return
     */
    private static boolean isValidComparisonTypes(InterfaceName metricsName,
            List<TimeFilter> timeFilters) {
        String ctLookupKey = buildComparisonTypeLookupKey(
                timeFilters.stream().map(tf -> tf.getRelation()).collect(Collectors.toList()));
        if (ctLookupKey == null || !COMPARISON_TYPE.get(metricsName).contains(ctLookupKey)) {
            List<String> expectedTypes = new ArrayList<>();
            COMPARISON_TYPE.get(metricsName).forEach(ct -> {
                expectedTypes.add(ct);
            });
            throw new LedpException(LedpCode.LEDP_40032,
                    new String[] { String.format(
                            "%s metrics should have comparison type as %s but found %s",
                            metricsName, String.join(" or ", expectedTypes), ctLookupKey) });
        }
        return true;
    }

    private static String buildComparisonTypeLookupKey(ComparisonType... comparisonTypes) {
        return buildComparisonTypeLookupKey(Arrays.asList(comparisonTypes));
    }

    private static String buildComparisonTypeLookupKey(List<ComparisonType> comparisonTypes) {
        if (CollectionUtils.isEmpty(comparisonTypes)) {
            return null;
        }
        return String.join(",",
                comparisonTypes.stream().map(ct -> ct == null ? "null" : ct.name()).sorted()
                        .collect(Collectors.toList()));
    }

    /**
     * For numerical boundaries for SpendChange, if there is positive boundaries
     * together with negative boundaries (numerical boundaries are sorted),
     * force to insert a zero in between
     *
     * @param bounds
     * @return
     */
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
}
