package com.latticeengines.query.factory.sqlquery;

import javax.sql.DataSource;

import com.querydsl.sql.Configuration;

/**
 * Implementation for PrestoDB
 *
 */
public class PrestoSQLQueryFactory extends BaseSQLQueryFactory {

    public PrestoSQLQueryFactory(Configuration configuration, DataSource dataSource) {
        super(configuration, dataSource);
    }

    @Override
    public BaseSQLQuery<?> query() {
        return new PrestoSQLQuery<Void>(connection, configuration);
    }

}
