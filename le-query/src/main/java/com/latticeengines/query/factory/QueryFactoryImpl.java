package com.latticeengines.query.factory;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;

@Component("queryFactory")
public class QueryFactoryImpl implements QueryFactory {

    @Inject
    private List<QueryProvider> queryProviders;

    public SQLQuery<?> getQuery(AttributeRepository repository, String sqlUser) {
        for (QueryProvider provider : queryProviders) {
            if (provider.providesQueryAgainst(repository)) {
                return provider.getQuery(repository, sqlUser);
            }
        }
        throw new RuntimeException(String.format("Could not find QueryProvider for specified data collection %s",
                repository.getCollectionName()));
    }

    public SQLQueryFactory getSQLQueryFactory(AttributeRepository repository, String sqlUser) {
        for (QueryProvider provider : queryProviders) {
            if (provider.providesQueryAgainst(repository)) {
                return provider.getCachedSQLQueryFactory(repository, sqlUser);
            }
        }
        throw new RuntimeException(String.format("Could not find QueryProvider for specified data collection %s",
                repository.getCollectionName()));
    }

}
