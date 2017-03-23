package com.latticeengines.query.exposed.object;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
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
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private QueryFactory queryFactory;

    @Autowired
    protected QueryEvaluator queryEvaluator;

    public abstract SchemaInterpretation getObjectType();

    public Predicate processFreeFormSearch(DataCollection dataCollection, String freeFormRestriction) {
        if (!StringUtils.isEmpty(freeFormRestriction)) {
            throw new LedpException(LedpCode.LEDP_37002, new String[] { getClass().getName() });
        }
        return Expressions.TRUE;
    }

    public final SQLQuery<?> startQuery(DataCollection dataCollection) {
        StringPath path = QueryUtils.getTablePath(getTable(dataCollection));
        SQLQuery<?> sqlQuery = queryFactory.getQuery(dataCollection).from(path);

        return sqlQuery;
    }

    public final Table getTable(DataCollection collection) {
        return collection.getTable(getObjectType());
    }

    public final long getCount(Query query) {
        try (PerformanceTimer timer = new PerformanceTimer(getClass().getName() + ".getCount")) {
            query.setObjectType(getObjectType());
            query.setPageFilter(null);
            query.setSort(null);
            return queryEvaluator.evaluate(getDataCollection(), query).fetchCount();
        }
    }

    public final DataPage getData(Query query) {
        try (PerformanceTimer timer = new PerformanceTimer(getClass().getName() + ".getData")) {
            query.setObjectType(getObjectType());
            return queryEvaluator.getResults(getDataCollection(), query);
        }
    }

    protected final DataCollection getDataCollection() {
        return dataCollectionProxy.getDataCollectionByType(MultiTenantContext.getTenant().getId(),
                DataCollectionType.Segmentation);
    }
}
