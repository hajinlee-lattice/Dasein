package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DateValueLookup;
import com.latticeengines.query.util.ExpressionTemplateUtils;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class DateValueResolver extends BaseLookupResolver<DateValueLookup> implements LookupResolver<DateValueLookup> {

    DateValueResolver(AttributeRepository repository) {
        super(repository);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ComparableExpression<? extends Comparable>> resolveForCompare(DateValueLookup lookup) {
        return Collections.singletonList(Expressions.dateTemplate(Date.class,
                ExpressionTemplateUtils.getDateTemplateValueOnPeriod(lookup.getPeriod(),
                        Integer.valueOf(lookup.getValue().toString()), ExpressionTemplateUtils.getCurrentDate())));
    }

}
