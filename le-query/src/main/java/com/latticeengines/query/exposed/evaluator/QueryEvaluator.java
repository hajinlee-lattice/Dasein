package com.latticeengines.query.exposed.evaluator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public abstract class QueryEvaluator {
    private static Map<Class<?>, QueryEvaluator> evaluators = new HashMap<>();

    protected QueryEvaluator(Class<?> storageClass) {
        evaluators.put(storageClass, this);
    }

    public static QueryEvaluator getEvaluator(Class<?> storageClass) {
        return evaluators.get(storageClass);
    }

    public abstract List<Map<String, Object>> getDataPage(DataCollection dataCollection, Restriction restriction);

    public abstract int count(DataCollection dataCollection, Restriction restriction);
}