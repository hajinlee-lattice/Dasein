package com.latticeengines.query.factory.sqlquery;

import javax.sql.DataSource;

import com.querydsl.sql.Configuration;

/**
 * Implementation for RedshiftSQL 
 *
 */
public class RedshiftSQLQueryFactory extends BaseSQLQueryFactory {

    public RedshiftSQLQueryFactory(Configuration configuration, DataSource dataSource) {
        super(configuration, dataSource);
    }

    @Override
    public BaseSQLQuery<?> query() {
        return new RedshiftSQLQuery<Void>(connection, configuration);
    }

}
