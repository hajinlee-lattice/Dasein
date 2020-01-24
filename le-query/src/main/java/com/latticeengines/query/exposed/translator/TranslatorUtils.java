package com.latticeengines.query.exposed.translator;

import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import com.latticeengines.domain.exposed.query.ComparisonType;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;

public final class TranslatorUtils {

    protected TranslatorUtils() {
        throw new UnsupportedOperationException();
    }
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static BooleanExpression toAggregatedBooleanExpression(StringPath stringPath, ComparisonType cmp, List<Object> values) {
        NumberPath numberPath = Expressions.numberPath(BigDecimal.class, stringPath.getMetadata());

        switch (cmp) {
        case GTE_AND_LTE:
            return numberPath.sum().goe(new BigDecimal(values.get(0).toString())).and(
                    numberPath.sum().loe(new BigDecimal(values.get(1).toString())));
        case GT_AND_LTE:
            return numberPath.sum().gt(new BigDecimal(values.get(0).toString())).and(
                    numberPath.sum().loe(new BigDecimal(values.get(1).toString())));
        case GT_AND_LT:
            return numberPath.sum().gt(new BigDecimal(values.get(0).toString())).and(
                    numberPath.sum().lt(new BigDecimal(values.get(1).toString())));
        case GTE_AND_LT:
            return numberPath.sum().goe(new BigDecimal(values.get(0).toString())).and(
                    numberPath.sum().lt(new BigDecimal(values.get(1).toString())));
        case GREATER_OR_EQUAL:
            return numberPath.sum().goe(new BigDecimal(values.get(0).toString()));
        case GREATER_THAN:
            return numberPath.sum().gt(new BigDecimal(values.get(0).toString()));
        case LESS_THAN:
            return numberPath.sum().lt(new BigDecimal(values.get(0).toString()));
        case LESS_OR_EQUAL:
            return numberPath.sum().loe(new BigDecimal(values.get(0).toString()));
        case EQUAL:
            return numberPath.sum().eq(new BigDecimal(values.get(0).toString()));
        default:
            throw new UnsupportedOperationException("Unsupported comparison type " + cmp);
        }
    }
}
