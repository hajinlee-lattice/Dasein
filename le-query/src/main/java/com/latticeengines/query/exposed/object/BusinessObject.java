package com.latticeengines.query.exposed.object;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;

@Deprecated
public abstract class BusinessObject {

    @Autowired
    protected QueryEvaluator queryEvaluator;

    public final long getCount(Query query) {
        try (PerformanceTimer timer = new PerformanceTimer(getClass().getName() + ".getCount")) {
            return 0L;
        }
    }

    public final DataPage getData(Query query) {
        try (PerformanceTimer timer = new PerformanceTimer(getClass().getName() + ".getData")) {
            return null;
        }
    }
}
