package com.latticeengines.query.exposed.object;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.querydsl.sql.SQLQuery;

public abstract class BusinessObject {
    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private QueryFactory queryFactory;

    @Autowired
    protected QueryEvaluator queryEvaluator;

    public abstract SchemaInterpretation getObjectType();

    public long getCount() {
        Query query = new Query();
        query.setObjectType(getObjectType());
        return queryEvaluator.evaluate(getDataCollection(), query).fetchCount();
    }

    protected SQLQuery<?> startCustomQuery() {
        return queryFactory.getQuery(getDataCollection());
    }

    protected DataCollection getDataCollection() {
        return metadataProxy.getDefaultDataCollection(MultiTenantContext.getTenant().getId());
    }
}
