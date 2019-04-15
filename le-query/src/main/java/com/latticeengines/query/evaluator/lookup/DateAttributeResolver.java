package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DateAttributeLookup;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.factory.sqlquery.BaseSQLQuery;
import com.latticeengines.query.factory.sqlquery.DateTimeExpressionTemplate;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class DateAttributeResolver extends AttributeResolver<DateAttributeLookup>
        implements LookupResolver<DateAttributeLookup> {

    public DateAttributeResolver(AttributeRepository repository, QueryProcessor queryProcessor, String sqlUser) {
        super(repository, queryProcessor, sqlUser);
    }

    @Override
    public Expression<?> resolveForSelect(DateAttributeLookup lookup, boolean asAlias) {
        if (lookup.getEntity() == null) {
            return QueryUtils.getAttributePath(lookup.getAttribute());
        }
        ColumnMetadata cm = getColumnMetadata(lookup);
        if (cm == null) {
            throw new QueryEvaluationException("Cannot find the attribute " + lookup + " in attribute repository.");
        }
        return resolveForDate(lookup, cm, lookup.getPeriod(), false);
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(DateAttributeLookup lookup) {
        if (lookup.getEntity() == null) {
            return Collections.singletonList(QueryUtils.getAttributePath(lookup.getAttribute()));
        }
        ColumnMetadata cm = getColumnMetadata(lookup);
        if (cm == null) {
            throw new IllegalArgumentException("Cannot find the attribute " + lookup + " in attribute repository.");
        }
        return Collections.singletonList((ComparableExpression<? extends Comparable<?>>) resolveForDate(lookup, cm, lookup.getPeriod(), false));
    }

    private Expression<? extends Comparable<?>> resolveForDate(DateAttributeLookup lookup, ColumnMetadata cm, String p,
            boolean alias) {
        BaseSQLQuery<?> query = queryProcessor.getQueryFactory().getQuery(repository, sqlUser);
        DateTimeExpressionTemplate dateTimeExpressionTemplate = query.getDateTimeTemplate();
        String datePath = lookup.toString();
        if (cm.getJavaClass() == null || cm.getJavaClass().equals((String.class.getSimpleName()))) {
            datePath = dateTimeExpressionTemplate.strAttrToDate(datePath);
        }
        return Expressions.dateTemplate(Date.class, dateTimeExpressionTemplate.getDateOnPeriodTemplate(p, datePath));
    }

}
