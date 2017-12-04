package com.latticeengines.query.exposed.translator;

import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.StringPath;

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
}
