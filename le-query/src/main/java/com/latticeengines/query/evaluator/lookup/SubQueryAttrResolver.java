package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

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

    public SubQueryAttrResolver(AttributeRepository repository,
                                QueryProcessor queryProcessor) {
        super(repository);
        this.queryProcessor = queryProcessor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ComparableExpression<String> resolveForSubselect(SubQueryAttrLookup lookup) {
        SubQuery subQuery = lookup.getSubQuery();
        SQLQuery<?> sqlSubQuery = queryProcessor.process(repository, subQuery.getQuery());
        String alias = subQuery.getAlias();
        StringPath subQueryPath = QueryUtils.getAttributePath(lookup.getSubQuery(), lookup.getAttribute());
        ComparableExpression<String> s =
                Expressions.asComparable(SQLExpressions.select(subQueryPath).from(sqlSubQuery.as(alias)));
        return s;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ComparableExpression<String>> resolveForCompare(SubQueryAttrLookup lookup) {
        return Collections
                .singletonList(Expressions.asComparable((Expression<String>) resolveForSelect(lookup, false)));
    }

    @Override
    public Expression<?> resolveForSelect(SubQueryAttrLookup lookup, boolean asAlias) {
        return QueryUtils.getAttributePath(lookup.getSubQuery(), lookup.getAttribute());
    }

}
