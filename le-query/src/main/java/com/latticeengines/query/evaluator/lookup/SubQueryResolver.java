package com.latticeengines.query.evaluator.lookup;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryLookup;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.util.AttrRepoUtils;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

public class SubQueryResolver extends BaseLookupResolver<SubQueryLookup>
        implements LookupResolver<SubQueryLookup> {

    private QueryProcessor queryProcessor;

    public SubQueryResolver(AttributeRepository repository,
                            QueryProcessor queryProcessor) {
        super(repository);
        this.queryProcessor = queryProcessor;
    }

    @Override
    public SQLQuery<?> resolveForWith(SubQuery subQuery, SQLQuery<?> sqlQuery) {
        SQLQuery<?> sqlSubQuery = queryProcessor.process(repository, subQuery.getQuery());
        StringPath tablePath = AttrRepoUtils.getTablePath(subQuery.getAlias());
        return sqlQuery.with(tablePath, sqlSubQuery);
    }
}
