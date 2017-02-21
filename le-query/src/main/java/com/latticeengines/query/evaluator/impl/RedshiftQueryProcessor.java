package com.latticeengines.query.evaluator.impl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.query.Query;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.domain.exposed.metadata.DataCollection;

@Component("redshiftQueryProcessor")
public class RedshiftQueryProcessor extends QueryProcessor {

    @Override
    public boolean canQuery(DataCollection dataCollection) {
        return false;
    }

    @Override
    public List<Map<String, Object>> getDataPage(DataCollection dataCollection, Query restriction) {
        return null;
    }

    @Override
    public int getCount(DataCollection dataCollection, Restriction restriction) {
        return 0;
    }

}
