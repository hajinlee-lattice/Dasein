package com.latticeengines.query.exposed.evaluator;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.query.Query;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.query.evaluator.impl.QueryProcessor;

@Component("queryEvaluator")
public class QueryEvaluator {
    @Autowired
    private List<QueryProcessor> processors;

    public List<Map<String, Object>> getDataPage(DataCollection dataCollection, Query query) {
        QueryProcessor processor = getProcessor(dataCollection);
        return processor.getDataPage(dataCollection, query);
    }

    public int getCount(DataCollection dataCollection, Restriction restriction) {
        QueryProcessor processor = getProcessor(dataCollection);
        return processor.getCount(dataCollection, restriction);
    }

    private QueryProcessor getProcessor(DataCollection dataCollection) {
        for (QueryProcessor processor : processors) {
            if (processor.canQuery(dataCollection)) {
                return processor;
            }
        }
        throw new IllegalStateException(String.format("Cannot find a QueryProcessor for dataCollection %s",
                dataCollection));
    }
}
