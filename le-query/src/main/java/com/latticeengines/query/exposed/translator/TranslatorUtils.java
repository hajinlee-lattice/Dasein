package com.latticeengines.query.exposed.translator;

import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.WindowFunction;

public class TranslatorUtils {
    public static String generateAlias(String prefix) {
        return prefix + RandomStringUtils.randomAlphanumeric(8);
    }

    public static BooleanExpression toBooleanExpression(StringPath numberPath, ComparisonType cmp, List<Object> values) {
        switch (cmp) {
        case GTE_AND_LTE:
            return numberPath.goe(values.get(0).toString()).and(numberPath.loe(values.get(1).toString()));
        case GT_AND_LTE:
            return numberPath.gt(values.get(0).toString()).and(numberPath.loe(values.get(1).toString()));
        case GT_AND_LT:
            return numberPath.gt(values.get(0).toString()).and(numberPath.lt(values.get(1).toString()));
        case GTE_AND_LT:
            return numberPath.goe(values.get(0).toString()).and(numberPath.lt(values.get(1).toString()));
        case GREATER_OR_EQUAL:
            return numberPath.goe(values.get(0).toString());
        case GREATER_THAN:
            return numberPath.gt(values.get(0).toString());
        case LESS_THAN:
            return numberPath.lt(values.get(0).toString());
        case LESS_OR_EQUAL:
            return numberPath.loe(values.get(0).toString());
        case EQUAL:
            return numberPath.eq(values.get(0).toString());
        default:
            throw new UnsupportedOperationException("Unsupported comparison type " + cmp);
        }
    }

    public static boolean isNotLessThanOperation(ComparisonType cmp) {
        return (ComparisonType.LESS_THAN != cmp && ComparisonType.LESS_OR_EQUAL != cmp);
    }

    public static BooleanExpression translateAggregatePredicate(StringPath aggr, AggregationFilter aggregationFilter) {
        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        List<Object> values = aggregationFilter.getValues();

        BooleanExpression aggrPredicate = null;
        switch (aggregateType) {
        case SUM:
        case AVG:
        case AT_LEAST_ONCE:
        case EACH:
            aggrPredicate = toBooleanExpression(aggr, cmp, values);
            break;
        }

        return aggrPredicate;
    }

    @SuppressWarnings("unchecked")
    public static WindowFunction translateEver(WindowFunction windowAgg) {
        return windowAgg.rows().between().unboundedPreceding().currentRow();
    }

    @SuppressWarnings("unchecked")
    public static WindowFunction translatePrior(WindowFunction windowAgg,
                                                int priorOffset) {
        return windowAgg.rows().between().unboundedPreceding().preceding(priorOffset);
    }

    @SuppressWarnings("unchecked")
    public static WindowFunction translateBetween(WindowFunction windowAgg,
                                                  int startOffset, int endOffset,
                                                  boolean preceding) {
        // for row preceding, SQL requires we start with the larger offset, order matters
        int minOffset = Math.min(startOffset, endOffset);
        int maxOffset = Math.max(startOffset, endOffset);
        if (preceding) {
            return windowAgg.rows().between().preceding(maxOffset).preceding(minOffset);
        } else {
            return windowAgg.rows().between().following(minOffset).following(maxOffset);
        }
    }

    public static WindowFunction translateTimeWindow(TimeFilter timeFilter, WindowFunction windowAgg) {
        ComparisonType type = timeFilter.getRelation();
        if (ComparisonType.EVER == type) {
            return translateEver(windowAgg);
        } else if (ComparisonType.BETWEEN == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    Integer.valueOf(timeFilter.getValues().get(1).toString()),
                                    true);
        } else if (ComparisonType.WITHIN == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    0,
                                    true);
        } else if (ComparisonType.PRIOR == type) {
            return translatePrior(windowAgg,
                                  Integer.valueOf(timeFilter.getValues().get(0).toString()));
        } else if (ComparisonType.FOLLOWING == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    false);
        } else if (ComparisonType.IN_CURRENT_PERIOD == type) {
            return translateBetween(windowAgg, 0, 0, true);
        } else if (ComparisonType.EQUAL == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    true);
        } else {
            throw new UnsupportedOperationException("Unsupported time filter type " + type);
        }
    }

    @SuppressWarnings("unchecked")
    public static WindowFunction translateAggregateTimeWindow(StringPath keysAccountId,
                                                              StringPath keysPeriodId,
                                                              StringPath trxnVal,
                                                              TimeFilter timeFilter,
                                                              AggregationFilter aggregationFilter,
                                                              boolean ascendingPeriod) {
        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnVal.getMetadata());


        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        boolean isNotLessComparison = isNotLessThanOperation(cmp);

        WindowFunction windowAgg = null;
        switch (aggregateType) {
        case SUM:
            windowAgg = SQLExpressions.sum(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            break;
        case AVG:
            windowAgg = SQLExpressions.avg(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            break;
        case AT_LEAST_ONCE:
            if (isNotLessComparison) {
                windowAgg = SQLExpressions.max(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            } else {
                windowAgg = SQLExpressions.min(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            }
            break;
        case EACH:
            if (isNotLessComparison) {
                windowAgg = SQLExpressions.min(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            } else {
                windowAgg = SQLExpressions.max(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            }
            break;
        }

        if (ascendingPeriod) {
            windowAgg.partitionBy(keysAccountId).orderBy(keysPeriodId);
        } else {
            windowAgg.partitionBy(keysAccountId).orderBy(keysPeriodId.desc());
        }

        return translateTimeWindow(timeFilter, windowAgg);
    }
}
