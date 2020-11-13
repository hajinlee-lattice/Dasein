package com.latticeengines.query.factory;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.sqlquery.BaseSQLQuery;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;

public abstract class QueryProvider implements ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(QueryProvider.class);

    private static final int MAX_CACHE_SIZE = 10000;
    private Cache<String, BaseSQLQueryFactory> factoryCache;

    protected ApplicationContext applicationContext;

    @PostConstruct
    private void init() {
        factoryCache = Caffeine.newBuilder().initialCapacity(MAX_CACHE_SIZE).build();
    }

    public abstract boolean providesQueryAgainst(AttributeRepository repository, String sqlUser);

    public BaseSQLQuery<?> getQuery(AttributeRepository repository, String sqlUser) {
        return getCachedSQLQueryFactory(repository, sqlUser).query();
    }

    public BaseSQLQueryFactory getCachedSQLQueryFactory(AttributeRepository repository, String sqlUser) {
        String cacheKey = getCacheKey(repository, sqlUser);
        BaseSQLQueryFactory factory = factoryCache.getIfPresent(cacheKey);
        if (factory != null) {
            return factory;
        } else {
            factory = getSQLQueryFactory(repository, sqlUser);
            factoryCache.put(cacheKey, factory);
            log.info("Created a query factory for attr-repo {}", cacheKey);
            return factory;
        }
    }

    protected abstract  BaseSQLQueryFactory getSQLQueryFactory(AttributeRepository repository, String sqlUser);

    protected abstract String getCacheKey(AttributeRepository repository, String sqlUser);

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
