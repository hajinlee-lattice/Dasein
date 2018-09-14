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
    private String sqlUser;

    public SelectAllResolver(AttributeRepository repository, QueryFactory queryFactory, String sqlUser) {
        super(repository);
        this.queryFactory = queryFactory;
        this.sqlUser = sqlUser;
    }

    @Override
    public SQLQuery<?> resolveForFrom(SelectAllLookup lookup) {
        StringPath tablePath = AttrRepoUtils.getTablePath(lookup.getAlias());
        return queryFactory.getQuery(repository, sqlUser).select(SQLExpressions.all).from(tablePath);
    }
}
