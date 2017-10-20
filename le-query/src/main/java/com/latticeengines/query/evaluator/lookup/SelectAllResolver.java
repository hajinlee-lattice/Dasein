package com.latticeengines.query.evaluator.lookup;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.SelectAllLookup;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.util.AttrRepoUtils;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;

public class SelectAllResolver extends BaseLookupResolver<SelectAllLookup> implements LookupResolver<SelectAllLookup> {
    private QueryFactory queryFactory;

    public SelectAllResolver(AttributeRepository repository, QueryFactory queryFactory) {
        super(repository);
        this.queryFactory = queryFactory;
    }

    @Override
    public SQLQuery<?> resolveForFrom(SelectAllLookup lookup) {
        StringPath tablePath = AttrRepoUtils.getTablePath(lookup.getAlias());
        return queryFactory.getQuery(repository).select(SQLExpressions.all).from(tablePath);
    }
}
