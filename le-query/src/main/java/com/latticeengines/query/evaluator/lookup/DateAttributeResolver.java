package com.latticeengines.query.evaluator.lookup;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DateAttributeLookup;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.query.util.ExpressionTemplateUtils;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

import edu.emory.mathcs.backport.java.util.Collections;

public class DateAttributeResolver extends AttributeResolver<DateAttributeLookup>
        implements LookupResolver<DateAttributeLookup> {

    public DateAttributeResolver(AttributeRepository repository) {
        super(repository);
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
        return resolveForDate(lookup.getEntity(), cm, lookup.getPeriod(), false);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(DateAttributeLookup lookup) {
        if (lookup.getEntity() == null) {
            return Collections.singletonList(QueryUtils.getAttributePath(lookup.getAttribute()));
        }
        ColumnMetadata cm = getColumnMetadata(lookup);
        if (cm == null) {
            throw new IllegalArgumentException("Cannot find the attribute " + lookup + " in attribute repository.");
        }
        return Collections.singletonList(resolveForDate(lookup.getEntity(), cm, lookup.getPeriod(), false));
    }

    private Expression<? extends Comparable<?>> resolveForDate(BusinessEntity entity, ColumnMetadata cm, Period p,
            boolean alias) {
        String datePath = entity.name() + "." + cm.getName();
        if (cm.getJavaClass() == null || cm.getJavaClass().equals((String.class.getSimpleName()))) {
            datePath = ExpressionTemplateUtils.strAttrToDate(datePath);
        }
        return Expressions.dateTemplate(Date.class, ExpressionTemplateUtils.getDateOnPeriodTemplate(p, datePath));
    }

}
