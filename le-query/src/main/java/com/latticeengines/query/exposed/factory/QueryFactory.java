package com.latticeengines.query.exposed.factory;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.sqlquery.BaseSQLQuery;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;

public interface QueryFactory {

    BaseSQLQuery<?> getQuery(AttributeRepository repository, String sqlUser);

    BaseSQLQueryFactory getSQLQueryFactory(AttributeRepository repository, String sqlUser);

}
