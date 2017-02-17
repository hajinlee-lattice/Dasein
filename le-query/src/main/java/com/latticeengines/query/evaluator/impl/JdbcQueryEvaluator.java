package com.latticeengines.query.evaluator.impl;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;

public class JdbcQueryEvaluator extends QueryEvaluator {

    protected JdbcQueryEvaluator() {
        super(JdbcStorage.class);
    }

    @Override
    public List<Map<String, Object>> getDataPage(DataCollection dataCollection, Restriction restriction) {
        return null;
    }

    @Override
    public int count(DataCollection dataCollection, Restriction restriction) {
        return 0;
    }
}
