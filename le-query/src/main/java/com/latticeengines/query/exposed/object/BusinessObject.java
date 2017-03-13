package com.latticeengines.query.exposed.object;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.util.QueryUtils;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

public abstract class BusinessObject {
    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private QueryFactory queryFactory;

    @Autowired
    protected QueryEvaluator queryEvaluator;

    public abstract SchemaInterpretation getObjectType();

    public SQLQuery<?> startQuery(DataCollection dataCollection, Query query) {
        Table table = dataCollection.getTable(query.getObjectType());
        StringPath path = QueryUtils.getTablePath(table);
        return queryFactory.getQuery(dataCollection).from(path);
    }

    public Predicate processFreeFormSearch(DataCollection dataCollection, String freeFormRestriction) {
        if (!StringUtils.isEmpty(freeFormRestriction)) {
            throw new RuntimeException("Must implement BusinessObject.processFreeFormSearch");
        }
        return Expressions.TRUE;
    }

    public final Table getTable(DataCollection collection) {
        return collection.getTable(getObjectType());
    }

    public final long getCount(Query query) {
        query.setObjectType(getObjectType());
        query.setPageFilter(null);
        query.setSort(null);
        return queryEvaluator.evaluate(getDataCollection(), query).fetchCount();
    }

    public final DataPage getData(Query query) {
        query.setObjectType(getObjectType());
        return queryEvaluator.getResults(getDataCollection(), query);
    }

    protected final DataCollection getDataCollection() {
        return metadataProxy.getDataCollectionByType(MultiTenantContext.getTenant().getId(),
                DataCollectionType.Segmentation);
    }
}
