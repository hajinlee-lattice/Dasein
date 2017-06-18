package com.latticeengines.query.factory;

import javax.annotation.PostConstruct;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;

public abstract class QueryProvider {

    private static final long MAX_CACHE_SIZE = 10000;
    private Cache<String, SQLQueryFactory> factoryCache;

    @PostConstruct
    private void init() {
        factoryCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
    }

    public abstract boolean providesQueryAgainst(AttributeRepository repository);

    public SQLQuery<?> getQuery(AttributeRepository repository) {
        SQLQueryFactory factory = factoryCache.getIfPresent(repository.getIdentifier());
        if (factory != null) {
            return factory.query();
        } else {
            factory = getSQLQueryFactory();
            factoryCache.put(repository.getIdentifier(), factory);
            return factory.query();
        }
    }

    protected abstract SQLQueryFactory getSQLQueryFactory();
}
