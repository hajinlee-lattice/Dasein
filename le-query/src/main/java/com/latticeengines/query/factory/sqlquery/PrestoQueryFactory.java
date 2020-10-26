package com.latticeengines.query.factory.sqlquery;

import javax.sql.DataSource;

import com.querydsl.sql.Configuration;

/**
 * Implementation for PrestoDB
 *
 */
public class PrestoQueryFactory extends BaseSQLQueryFactory {

    public PrestoQueryFactory(Configuration configuration, DataSource dataSource) {
        super(configuration, dataSource);
    }

    @Override
    public BaseSQLQuery<?> query() {
        return new PrestoQuery<Void>(connection, configuration);
    }

}
