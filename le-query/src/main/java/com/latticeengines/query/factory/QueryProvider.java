package com.latticeengines.query.factory;

import javax.annotation.PostConstruct;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;

public abstract class QueryProvider {

    private static final long MAX_CACHE_SIZE = 10000;

    @PostConstruct
    private void init() {
        factoryCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
    }

    public abstract boolean providesQueryAgainst(DataCollection dataCollection);

    public SQLQuery<?> getQuery(DataCollection dataCollection) {
        SQLQueryFactory factory = factoryCache.getIfPresent(dataCollection.getName());
        if (factory != null) {
            return factory.query();
        } else {
            factory = getSQLQueryFactory();
            factoryCache.put(dataCollection.getName(), factory);
            return factory.query();
        }
    }

    protected abstract SQLQueryFactory getSQLQueryFactory();

    protected Cache<String, SQLQueryFactory> factoryCache;
}
