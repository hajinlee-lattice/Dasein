package com.latticeengines.query.evaluator.impl;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.query.Query;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public abstract class QueryProcessor {

    public abstract boolean canQuery(DataCollection dataCollection);

    public abstract List<Map<String, Object>> getDataPage(DataCollection dataCollection, Query restriction);

    public abstract int getCount(DataCollection dataCollection, Restriction restriction);
}