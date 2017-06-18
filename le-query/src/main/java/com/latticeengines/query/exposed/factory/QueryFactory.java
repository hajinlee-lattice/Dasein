package com.latticeengines.query.exposed.factory;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.QueryProvider;
import com.querydsl.sql.SQLQuery;

@Component("queryFactory")
public class QueryFactory {

    @Autowired
    private List<QueryProvider> queryProviders;

    public SQLQuery<?> getQuery(AttributeRepository repository) {
        for (QueryProvider provider : queryProviders) {
            if (provider.providesQueryAgainst(repository)) {
                return provider.getQuery(repository);
            }
        }
        throw new RuntimeException(String.format("Could not find QueryProvider for specified data collection %s",
                repository.getCollectionName()));
    }
}
