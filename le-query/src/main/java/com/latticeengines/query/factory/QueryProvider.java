package com.latticeengines.query.factory;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;

public abstract class QueryProvider implements ApplicationContextAware {

    private static final int MAX_CACHE_SIZE = 10000;
    private Cache<String, SQLQueryFactory> factoryCache;

    protected DataSource dataSource;
    protected ApplicationContext applicationContext;

    @PostConstruct
    private void init() {
        factoryCache = Caffeine.newBuilder().initialCapacity(MAX_CACHE_SIZE).build();
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

    public SQLQueryFactory getCachedSQLQueryFactory(AttributeRepository repository) {
        SQLQueryFactory factory = factoryCache.getIfPresent(repository.getIdentifier());
        if (factory != null) {
            return factory;
        } else {
            factory = getSQLQueryFactory();
            factoryCache.put(repository.getIdentifier(), factory);
            return factory;
        }
    }

    protected abstract SQLQueryFactory getSQLQueryFactory();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
