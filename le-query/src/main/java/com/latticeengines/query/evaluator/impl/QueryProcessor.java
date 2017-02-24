package com.latticeengines.query.evaluator.impl;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.Query;
import com.querydsl.sql.SQLQuery;

public abstract class QueryProcessor {

    public abstract boolean canQuery(DataCollection dataCollection);

    public abstract SQLQuery<?> process(DataCollection dataCollection, Query restriction);
}