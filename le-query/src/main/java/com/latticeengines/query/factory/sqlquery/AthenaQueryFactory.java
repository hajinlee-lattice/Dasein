package com.latticeengines.query.factory.sqlquery;

import javax.sql.DataSource;

import com.querydsl.sql.Configuration;

/**
 * Implementation for AWS Athena
 *
 */
public class AthenaQueryFactory extends BaseSQLQueryFactory {

    public AthenaQueryFactory(Configuration configuration, DataSource dataSource) {
        super(configuration, dataSource);
    }

    @Override
    public BaseSQLQuery<?> query() {
        return new AthenaQuery<>(connection, configuration);
    }

}
