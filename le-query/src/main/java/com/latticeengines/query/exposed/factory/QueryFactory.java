package com.latticeengines.query.exposed.factory;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.query.factory.QueryProvider;
import com.querydsl.sql.SQLQuery;

@Component("queryFactory")
public class QueryFactory {

    @Autowired
    private List<QueryProvider> queryProviders;

    public SQLQuery<?> getQuery(DataCollection dataCollection) {
        for (QueryProvider provider : queryProviders) {
            if (provider.providesQueryAgainst(dataCollection)) {
                return provider.getQuery(dataCollection);
            }
        }
        throw new RuntimeException(String.format("Could not find QueryProvider for specified dataCollection %s",
                dataCollection));
    }
}
