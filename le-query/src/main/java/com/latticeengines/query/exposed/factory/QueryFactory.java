package com.latticeengines.query.exposed.factory;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;

public interface QueryFactory {

    SQLQuery<?> getQuery(AttributeRepository repository);

    SQLQueryFactory getSQLQueryFactory(AttributeRepository repository);

}
