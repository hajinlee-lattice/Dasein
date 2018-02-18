package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;

public class SubQueryAttrResolver extends BaseLookupResolver<SubQueryAttrLookup>
        implements LookupResolver<SubQueryAttrLookup> {
    private QueryProcessor queryProcessor;

    public SubQueryAttrResolver(AttributeRepository repository, QueryProcessor queryProcessor) {
        super(repository);
        this.queryProcessor = queryProcessor;
    }

    @Override
    public ComparableExpression<String> resolveForSubselect(SubQueryAttrLookup lookup) {
        SubQuery subQuery = lookup.getSubQuery();
        SQLQuery<?> sqlSubQuery;
        if (subQuery.getSubQueryExpression() == null) {
            sqlSubQuery = queryProcessor.process(repository, subQuery.getQuery());
        } else {
            sqlSubQuery = (SQLQuery<?>) subQuery.getSubQueryExpression();
        }
        String alias = subQuery.getAlias();
        StringPath subQueryPath = QueryUtils.getAttributePath(lookup.getSubQuery(), lookup.getAttribute());
        return Expressions.asComparable(SQLExpressions.select(subQueryPath).from(sqlSubQuery.as(alias)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(SubQueryAttrLookup lookup) {
        return Collections
                .singletonList(Expressions.asComparable((Expression<String>) resolveForSelect(lookup, false)));
    }

    @Override
    public Expression<?> resolveForSelect(SubQueryAttrLookup lookup, boolean asAlias) {
        if (StringUtils.isBlank(lookup.getAttribute())) {
            return Expressions.TRUE;
        } else {
            return QueryUtils.getAttributePath(lookup.getSubQuery(), lookup.getAttribute());
        }
    }

}
