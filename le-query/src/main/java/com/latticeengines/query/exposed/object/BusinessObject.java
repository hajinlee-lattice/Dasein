package com.latticeengines.query.exposed.object;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.factory.QueryFactory;
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

    public SQLQuery<?> getFromClause(DataCollection dataCollection, Query query) {
        String tableName = getTableName(dataCollection);
        StringPath path = Expressions.stringPath(tableName);
        return startQuery().from(path);
    }

    public Predicate processFreeFormSearch(DataCollection dataCollection, String freeFormRestriction) {
        if (!StringUtils.isEmpty(freeFormRestriction)) {
            throw new RuntimeException("Must implement BusinessObject.processFreeFormSearch");
        }
        return Expressions.TRUE;
    }

    public final Table getTable(DataCollection collection) {
        return collection.getTables().stream() //
                .filter(t -> getObjectType().toString().equals(t.getInterpretation())) //
                .findFirst().orElse(null);
    }

    protected final String getTableName(DataCollection collection) {
        Table table = getTable(collection);
        StorageMechanism storage = table.getStorageMechanism();
        String tableName = table.getName();
        if (storage instanceof JdbcStorage) {
            tableName = storage.getTableNameInStorage();
        }
        return tableName;
    }

    public final long getCount(Query query) {
        query.setObjectType(getObjectType());
        return queryEvaluator.evaluate(getDefaultDataCollection(), query).fetchCount();
    }

    public final List<Map<String, Object>> getData(Query query) {
        query.setObjectType(getObjectType());
        return queryEvaluator.getResults(getDefaultDataCollection(), query);
    }

    protected final SQLQuery<?> startQuery() {
        return queryFactory.getQuery(getDefaultDataCollection());
    }

    protected final DataCollection getDefaultDataCollection() {
        return metadataProxy.getDefaultDataCollection(MultiTenantContext.getTenant().getId());
    }
}
