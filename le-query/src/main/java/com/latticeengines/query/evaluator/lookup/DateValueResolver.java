package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DateValueLookup;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.factory.sqlquery.BaseSQLQuery;
import com.latticeengines.query.factory.sqlquery.DateTimeExpressionTemplate;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class DateValueResolver extends BaseLookupResolver<DateValueLookup> implements LookupResolver<DateValueLookup> {

    private QueryProcessor queryProcessor;
    private String sqlUser;

    DateValueResolver(AttributeRepository repository, QueryProcessor queryProcessor, String sqlUser) {
        super(repository);
        this.queryProcessor = queryProcessor;
        this.sqlUser = sqlUser;
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(DateValueLookup lookup) {
        BaseSQLQuery<?> query = queryProcessor.getQueryFactory().getQuery(repository, sqlUser);
        DateTimeExpressionTemplate dateTimeExpressionTemplate = query.getDateTimeTemplate();
        return Collections.singletonList(Expressions.dateTemplate(Date.class,
                dateTimeExpressionTemplate.getDateTargetValueOnPeriodTemplate(lookup.getPeriod(),
                        Integer.valueOf(lookup.getValue().toString()), dateTimeExpressionTemplate.getCurrentDate())));
    }

}
