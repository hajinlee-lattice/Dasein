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
}
