package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class StandardActivityMetricsAgg extends BaseAggregator<StandardActivityMetricsAgg.Context>
        implements Aggregator<StandardActivityMetricsAgg.Context> {

    private static final long serialVersionUID = -7883873860033640700L;

    private List<ActivityMetrics> metrics;
    // metrics name with period -> period boundaries
    private Map<String, List<Integer>> periodRanges;
    private List<String> groupByFields;

    public StandardActivityMetricsAgg(Fields fieldDeclaration, List<String> groupByFields,
            List<ActivityMetrics> metrics, Map<String, List<Integer>> periodRanges) {
        super(fieldDeclaration);
        this.metrics = metrics;
        this.periodRanges = periodRanges;
        this.groupByFields = groupByFields;
    }

    public static class Context extends BaseAggregator.Context {
        List<String> groupByVals;
        MarginContext marginContext;
        SpendChangeContext spendChangeContext;
        // metrics name with period -> context
        Map<String, TotalSpendOvertimeContext> totalSpendOvertimeContexts = new HashMap<>();
        // metrics name with period -> context
        Map<String, AvgSpendOvertimeContext> avgSpendOvertimeContexts = new HashMap<>();
        HasPurchasedContext hasPurchasedContext;
    }

    /**
     * Margin on product A purchase by a given account: Round (100* ((Revenue -
     * Cost) of product A sell within last X weeks / cost of product A.)) If
     * revenue or cost is not available, margin is not available
     */
    public static class MarginContext {
        double totalAmount = 0;
        double totalCost = 0;
        Pair<Integer, Integer> periodRange;
    }

    /**
     * Insight: % spend change is calculated by comparing average spend for a
     * given product of a given account in the specified time window with that
     * of the range in prior time window. If lastAvgSpend is not available,
     * spend change is -100% If previousAvgSpend is not available, spend change
     * is 100%
     */
    public static class SpendChangeContext {
        double lastPeriodTotalSpend = 0;
        double priorPeriodTotalSpend = 0;
        Pair<Integer, Integer> lastPeriodRange;
        Pair<Integer, Integer> priorPeriodRange;
    }

    public static class TotalSpendOvertimeContext {
        double totalAmount = 0;
        Pair<Integer, Integer> periodRange;
    }

    public static class AvgSpendOvertimeContext {
        double totalAmount = 0;
        Pair<Integer, Integer> periodRange;
    }

    public static class HasPurchasedContext {
        boolean hasPurchased = false;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.groupByVals = new ArrayList<>();
        groupByFields.forEach(groupByField -> {
            context.groupByVals.add(group.getString(groupByField));
        });
        for (ActivityMetrics m : metrics) {
            switch (m.getMetrics()) {
            case Margin:
                context.marginContext = new MarginContext();
                context.marginContext.periodRange = Pair.of(
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(0),
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(1));
                break;
            case SpendChange:
                context.spendChangeContext = new SpendChangeContext();
                context.spendChangeContext.lastPeriodRange = Pair.of(
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(0),
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(1));
                context.spendChangeContext.priorPeriodRange = Pair.of(
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(2),
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(3));
                break;
            case TotalSpendOvertime:
                TotalSpendOvertimeContext totalSpendOvertimeContext = new TotalSpendOvertimeContext();
                totalSpendOvertimeContext.periodRange = Pair.of(
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(0),
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(1));
                context.totalSpendOvertimeContexts.put(ActivityMetricsUtils.getNameWithPeriod(m),
                        totalSpendOvertimeContext);
                break;
            case AvgSpendOvertime:
                AvgSpendOvertimeContext avgSpendOvertimeContext = new AvgSpendOvertimeContext();
                avgSpendOvertimeContext.periodRange = Pair.of(
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(0),
                        periodRanges.get(ActivityMetricsUtils.getNameWithPeriod(m)).get(1));
                context.avgSpendOvertimeContexts.put(ActivityMetricsUtils.getNameWithPeriod(m),
                        avgSpendOvertimeContext);
                break;
            case HasPurchased:
                context.hasPurchasedContext = new HasPurchasedContext();
                break;
            default:
                throw new UnsupportedOperationException(m.getMetrics() + " metrics is not supported");
            }
        }

        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        for (ActivityMetrics m : metrics) {
            switch (m.getMetrics()) {
                case Margin:
                    context = updateMarginContext(context, arguments, m);
                    break;
                case SpendChange:
                    context = updateSpendChangeContext(context, arguments, m);
                    break;
                case TotalSpendOvertime:
                    context = updateTotalSpendOvertimeContext(context, arguments, m);
                    break;
                case AvgSpendOvertime:
                    context = updateAvgSpendOvertimeContext(context, arguments, m);
                    break;
                case HasPurchased:
                    context = updateHasPurchasedContext(context, arguments, m);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            m.getMetrics() + " metrics is not supported");
            }
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (int i = 0; i < groupByFields.size(); i++) {
            result.set(namePositionMap.get(groupByFields.get(i)), context.groupByVals.get(i));
        }
        for (ActivityMetrics m : metrics) {
            switch (m.getMetrics()) {
                case Margin:
                    result = finalizeMargin(result, context, m);
                    break;
                case SpendChange:
                    result = finalizeSpendChange(result, context, m);
                    break;
                case TotalSpendOvertime:
                    result = finalizeTotalSpendOvertime(result, context, m);
                    break;
                case AvgSpendOvertime:
                    result = finalizeAvgSpendOvertime(result, context, m);
                    break;
                case HasPurchased:
                    result = finalizeHasPurchased(result, context, m);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            m.getMetrics() + " metrics is not supported");
            }
        }

        return result;
    }

    private boolean isValidPeriod(TupleEntry arguments, Pair<Integer, Integer> periodRange) {
        Object periodId = arguments.getObject(InterfaceName.PeriodId.name());
        if (periodId != null && ((Integer) periodId) >= periodRange.getLeft()
                && ((Integer) periodId) <= periodRange.getRight()) {
            return true;
        } else {
            return false;
        }
    }

    private Context updateMarginContext(Context context, TupleEntry arguments,
            ActivityMetrics metrics) {
        if (!isValidPeriod(arguments, context.marginContext.periodRange)) {
            return context;
        }
        // treat null as 0 by design
        double amount = arguments.getDouble(InterfaceName.TotalAmount.name());
        double cost = arguments.getDouble(InterfaceName.TotalCost.name());
        context.marginContext.totalAmount += amount;
        context.marginContext.totalCost += cost;
        return context;
    }

    private Tuple finalizeMargin(Tuple result, Context context, ActivityMetrics metrics) {
        Integer margin = null;
        if (context.marginContext.totalAmount != 0 && context.marginContext.totalCost != 0) {
            margin = (int) Math.round(
                    100.0 * (context.marginContext.totalAmount - context.marginContext.totalCost)
                            / context.marginContext.totalAmount);
        }
        result.set(namePositionMap.get(ActivityMetricsUtils.getNameWithPeriod(metrics)), margin);
        return result;
    }

    private Context updateSpendChangeContext(Context context, TupleEntry arguments,
            ActivityMetrics metrics) {
        // treat null as 0 by design
        double amount = arguments.getDouble(InterfaceName.TotalAmount.name());
        if (isValidPeriod(arguments, context.spendChangeContext.lastPeriodRange)) {
            context.spendChangeContext.lastPeriodTotalSpend += amount;
        }
        if (isValidPeriod(arguments, context.spendChangeContext.priorPeriodRange)) {
            context.spendChangeContext.priorPeriodTotalSpend += amount;
        }
        return context;
    }

    private Tuple finalizeSpendChange(Tuple result, Context context, ActivityMetrics metrics) {
        double lastPeriodAvgSpend = context.spendChangeContext.lastPeriodTotalSpend
                / (context.spendChangeContext.lastPeriodRange.getRight()
                        - context.spendChangeContext.lastPeriodRange.getLeft() + 1);
        double priorPeriodAvgSpend = context.spendChangeContext.priorPeriodTotalSpend
                / (context.spendChangeContext.priorPeriodRange.getRight()
                        - context.spendChangeContext.priorPeriodRange.getLeft() + 1);
        int spendChange;
        if (lastPeriodAvgSpend == 0 && priorPeriodAvgSpend == 0) {
            spendChange = 0;
        } else if (priorPeriodAvgSpend != 0) {
            spendChange = (int) Math
                    .round(100 * (lastPeriodAvgSpend - priorPeriodAvgSpend) / priorPeriodAvgSpend);
        } else {
            spendChange = 100;
        }
        result.set(namePositionMap.get(ActivityMetricsUtils.getNameWithPeriod(metrics)),
                spendChange);
        return result;
    }

    private Context updateTotalSpendOvertimeContext(Context context, TupleEntry arguments,
            ActivityMetrics metrics) {
        TotalSpendOvertimeContext totalSpendOvertimeContext = context.totalSpendOvertimeContexts
                .get(ActivityMetricsUtils.getNameWithPeriod(metrics));
        if (!isValidPeriod(arguments, totalSpendOvertimeContext.periodRange)) {
            return context;
        }
        // treat null as 0 by design
        double amount = arguments.getDouble(InterfaceName.TotalAmount.name());
        totalSpendOvertimeContext.totalAmount += amount;
        return context;
    }

    private Tuple finalizeTotalSpendOvertime(Tuple result, Context context,
            ActivityMetrics metrics) {
        TotalSpendOvertimeContext totalSpendOvertimeContext = context.totalSpendOvertimeContexts
                .get(ActivityMetricsUtils.getNameWithPeriod(metrics));
        result.set(namePositionMap.get(ActivityMetricsUtils.getNameWithPeriod(metrics)),
                totalSpendOvertimeContext.totalAmount);
        return result;
    }

    private Context updateAvgSpendOvertimeContext(Context context, TupleEntry arguments,
            ActivityMetrics metrics) {
        AvgSpendOvertimeContext avgSpendOvertimeContext = context.avgSpendOvertimeContexts
                .get(ActivityMetricsUtils.getNameWithPeriod(metrics));
        if (!isValidPeriod(arguments, avgSpendOvertimeContext.periodRange)) {
            return context;
        }
        // treat null as 0 by design
        double amount = arguments.getDouble(InterfaceName.TotalAmount.name());
        avgSpendOvertimeContext.totalAmount += amount;
        return context;
    }

    private Tuple finalizeAvgSpendOvertime(Tuple result, Context context, ActivityMetrics metrics) {
        AvgSpendOvertimeContext avgSpendOvertimeContext = context.avgSpendOvertimeContexts
                .get(ActivityMetricsUtils.getNameWithPeriod(metrics));
        double avgSpendOvertime = avgSpendOvertimeContext.totalAmount
                / (avgSpendOvertimeContext.periodRange.getRight() - avgSpendOvertimeContext.periodRange.getLeft() + 1);
        result.set(namePositionMap.get(ActivityMetricsUtils.getNameWithPeriod(metrics)),
                avgSpendOvertime);
        return result;
    }

    private Context updateHasPurchasedContext(Context context, TupleEntry arguments,
            ActivityMetrics metrics) {
        context.hasPurchasedContext.hasPurchased = true;
        return context;
    }

    private Tuple finalizeHasPurchased(Tuple result, Context context, ActivityMetrics metrics) {
        result.set(namePositionMap.get(ActivityMetricsUtils.getNameWithPeriod(metrics)),
                context.hasPurchasedContext.hasPurchased);
        return result;
    }


}
